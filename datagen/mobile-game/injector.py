import asyncio
import datetime
import random
import time
import hashlib
import logging
import argparse
from typing import List, Optional

from src import UserScore, Publisher

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)

# fmt: off
COLORS = [
    "Magenta", "AliceBlue", "Almond", "Amaranth", "Amber", "Amethyst", "AndroidGreen",
    "AntiqueBrass", "Fuchsia", "Ruby", "AppleGreen", "Apricot", "Aqua", "ArmyGreen",
    "Asparagus", "Auburn", "Azure", "Banana", "Beige", "Bisque", "BarnRed", "BattleshipGrey",
]
ANIMALS = [
    "Echidna", "Koala", "Wombat", "Marmot", "Quokka", "Kangaroo", "Dingo", "Numbat",
    "Emu", "Wallaby", "CaneToad", "Bilby", "Possum", "Cassowary", "Kookaburra",
    "Platypus", "Bandicoot", "Cockatoo", "Antechinus",
]
# fmt: on
live_teams: List["TeamInfo"] = []
MAX_TEAM_ID_WIDTH = 10
BASE_DELAY_IN_MILLIS = 5 * 60 * 1000
FUZZY_DELAY_IN_MILLIS = 5 * 60 * 1000
BASE_TEAM_EXPIRATION_TIME_IN_MINS = 20
FUZZY_TEAM_EXPIRATION_TIME_IN_MINS = 20


class TeamInfo:
    """Represents a single team with its members, state, and metadata."""

    def __init__(
        self, team_name: str, start_time_millis: int, args: argparse.Namespace
    ):
        self.team_name = team_name
        self.start_time_millis = start_time_millis
        self.team_id = self._get_team_id()
        self._robot: Optional[str] = None

        if random.randint(0, args.robot_probability - 1) == 0:
            robot_num = 0
            self._robot = f"RBT-{robot_num:0{len(str(args.max_members_per_team - 1))}}-{self.team_id}"

        self.expiration_period = (
            random.randint(0, FUZZY_TEAM_EXPIRATION_TIME_IN_MINS)
            + BASE_TEAM_EXPIRATION_TIME_IN_MINS
        )
        self.num_members = random.randint(
            args.min_members_per_team, args.max_members_per_team
        )

    @property
    def end_time_millis(self) -> int:
        """Calculates the team's expiration timestamp in milliseconds."""
        return self.start_time_millis + (self.expiration_period * 60 * 1000)

    def _get_team_id(self) -> str:
        """Generates a fixed-width, deterministic team ID from its name."""
        hash_object = hashlib.sha1(self.team_name.encode("utf-8"))
        hex_digest = hash_object.hexdigest()
        numeric_id = int(hex_digest[:8], 16)
        return f"{numeric_id:0{MAX_TEAM_ID_WIDTH}}"

    @property
    def robot(self) -> Optional[str]:
        return self._robot

    def get_random_user(self, args: argparse.Namespace) -> str:
        """Generates a random user ID belonging to this team."""
        num = random.randint(0, self.num_members - 1)
        return f"USR-{num:0{len(str(args.max_members_per_team - 1))}}-{self.team_id}"

    def __str__(self) -> str:
        return f"({self.team_name}, members: {self.num_members}, robot: {self.robot})"


def add_live_team(args: argparse.Namespace):
    """Creates a new team and adds it to the global list of live teams."""
    team_name = f"{random.choice(COLORS)}-{random.choice(ANIMALS)}"
    new_team = TeamInfo(team_name, int(time.time() * 1000), args)
    live_teams.append(new_team)
    logging.info(f"Team added: {new_team}")


def random_team(args: argparse.Namespace) -> TeamInfo:
    """Selects a random team, replacing it if it has expired."""
    team = random.choice(live_teams)
    if team.end_time_millis < int(time.time() * 1000) or team.num_members == 0:
        logging.info(f"Team expired; replacing: {team.team_name} (id: {team.team_id})")
        live_teams.remove(team)
        add_live_team(args)
        return live_teams[-1]
    return team


def generate_event(
    args: argparse.Namespace,
    event_time_millis: int,
    event_type: str,
) -> UserScore:
    """Generates a single user score event for a random team and user."""
    team = random_team(args)
    user = None
    if team.robot and random.randint(0, (team.num_members // 2)) == 0:
        user = team.robot
    if user is None:
        user = team.get_random_user(args)

    return UserScore(
        user_id=user,
        team_id=team.team_id,
        team_name=team.team_name,
        score=random.randint(0, args.max_score),
        event_time_millis=event_time_millis,
        readable_time=datetime.datetime.fromtimestamp(
            event_time_millis / 1000
        ).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        event_type=event_type,
    )


async def run_simulation(args: argparse.Namespace):
    """Runs the main asynchronous loop for event generation and publishing."""
    logging.info(f"Starting Injector with configuration: {args}")

    publisher = None
    if args.output == "kafka":
        publisher = Publisher(
            args.bootstrap_servers, args.topic_name, args.schema_registry_url
        )

    while len(live_teams) < args.num_live_teams:
        add_live_team(args)

    event_counter = 0
    try:
        while True:
            wait_time = random.expovariate(args.avg_qps)
            await asyncio.sleep(wait_time)

            event_time = int(time.time() * 1000)
            event_type = "normal"
            if (
                event_counter > 0
                and args.late_event_rate > 0
                and event_counter % args.late_event_rate == 0
            ):
                logging.info("--- Generating late event ---")
                delay = BASE_DELAY_IN_MILLIS + random.randint(0, FUZZY_DELAY_IN_MILLIS)
                event_time -= delay
                event_type = "late"
            user_score = generate_event(args, event_time, event_type)
            if publisher is not None:
                publisher.publish(user_score)
            else:
                logging.info(user_score)

            event_counter += 1
    except KeyboardInterrupt:
        logging.warning("Injector stopped by user.")
    finally:
        publisher.close()


def main():
    # fmt: off
    parser = argparse.ArgumentParser(description="Mobile Gaming Event Injector.")
    parser.add_argument("--output", choices=["kafka", "print"], default="kafka", help="The output destination for events: 'kafka' or 'print'.")
    # Kafka-specific arguments
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092", help="Bootstrap server addresses.")
    parser.add_argument("--schema-registry-url", type=str, default="http://localhost:8081", help="Schema registry URL.")
    parser.add_argument("--topic-name", type=str, default="user-score", help="Kafka topic name")
    # General simulation arguments
    parser.add_argument("--avg-qps", type=float, default=20.0, help="Average events per second.")
    parser.add_argument("--robot-probability", type=int, default=2, help="Probability (1 in N) a team has a robot.")
    parser.add_argument("--num-live-teams", type=int, default=10, help="Number of active teams to maintain.")
    parser.add_argument("--min-members-per-team", type=int, default=5, help="Minimum number of members on a team.")
    parser.add_argument("--max-members-per-team", type=int, default=15, help="Maximum number of members on a team.")
    parser.add_argument("--max-score", type=int, default=20, help="Maximum score a user can get in one event.")
    parser.add_argument("--late-event-rate", type=int, default=0, help="Frequency (1 in N) of late data events.")
    # fmt: on

    args = parser.parse_args()
    asyncio.run(run_simulation(args))


if __name__ == "__main__":
    main()
