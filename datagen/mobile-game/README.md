# Mobile Gaming Event Injector (`injector.py`)

A Python port of Apache Beam's [**Mobile Gaming Example**](https://beam.apache.org/get-started/mobile-gaming-example/) data generator. This script simulates game events such as user scores, designed to support real-time analytics pipelines for leaderboards, scoring engines, and fraud detection.

## Key Features

- **Dynamic Game Simulation**
  Simulates gameplay with multiple teams and users. Teams are created and retired dynamically for a constantly evolving environment.

- **Late Event Injection**
  Mimics delayed data arrival due to network issues or buffering. Controlled using `--late-event-rate`.

- **Abuse & Bot Simulation**
  Randomly injects "robot" players with the `--robot-probability` flag to simulate fraud or abusive behavior.

- **Flexible Output**
  Can output to:

  - Kafka (with Avro + Schema Registry)
  - Console (for debugging)

## Setup & Usage

### Prerequisites

- Python 3.8+
- A running PostgreSQL instance
- (Optional) A running Kafka cluster if you intend to send records to Kafka.

### Installation & Running

1. Create and activate a virtual environment:

   ```bash
   python -m venv venv
   source venv/bin/activate
   ```

2. Install dependencies:

   ```bash
   pip install -r datagen/mobile-game/requirements.txt
   ```

3. Run the injector and view all available options:

   ```bash
   python datagen/mobile-game/injector.py --help
   ```

### Available CLI Options

```text
usage: injector.py [-h] [--output {kafka,print}] [--bootstrap-servers BOOTSTRAP_SERVERS] [--schema-registry-url SCHEMA_REGISTRY_URL]
                   [--topic-name TOPIC_NAME] [--avg-qps AVG_QPS] [--robot-probability ROBOT_PROBABILITY] [--num-live-teams NUM_LIVE_TEAMS]
                   [--min-members-per-team MIN_MEMBERS_PER_TEAM] [--max-members-per-team MAX_MEMBERS_PER_TEAM] [--min-score MIN_SCORE]
                   [--max-score MAX_SCORE] [--late-event-rate LATE_EVENT_RATE]

Mobile Gaming Event Injector.

options:
  -h, --help            show this help message and exit
  --output {kafka,print}
                        The output destination for events: 'kafka' or 'print'.
  --bootstrap-servers BOOTSTRAP_SERVERS
                        Bootstrap server addresses.
  --schema-registry-url SCHEMA_REGISTRY_URL
                        Schema registry URL.
  --topic-name TOPIC_NAME
                        Kafka topic name
  --avg-qps AVG_QPS     Average events per second.
  --robot-probability ROBOT_PROBABILITY
                        Probability (1 in N) a team has a robot.
  --num-live-teams NUM_LIVE_TEAMS
                        Number of active teams to maintain.
  --min-members-per-team MIN_MEMBERS_PER_TEAM
                        Minimum number of members on a team.
  --max-members-per-team MAX_MEMBERS_PER_TEAM
                        Maximum number of members on a team.
  --min-score MIN_SCORE
                        Minimum score a user can get in one event.
  --max-score MAX_SCORE
                        Maximum score a user can get in one event.
  --late-event-rate LATE_EVENT_RATE
                        Frequency (1 in N) of late data events.
```
