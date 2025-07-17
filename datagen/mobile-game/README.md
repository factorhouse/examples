## Mobile Gaming Event Injector (`injector.py`)

A Python port of Apache Beam's [**Mobile Gaming Example**](https://beam.apache.org/get-started/mobile-gaming-example/) data generator. This script simulates game events such as user scores, designed to support real-time analytics pipelines for leaderboards, scoring engines, and fraud detection.

### Key Features

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

### Setup & Usage

1. Create and activate a virtual environment:

   ```bash
   python -m venv venv
   source venv/bin/activate
   ```

2. Install dependencies:

   ```bash
   pip install -r datagen/mobile-game/requirements.txt
   ```

3. Run the injector:

   ```bash
   python datagen/mobile-game/injector.py --help
   ```

#### Available CLI Options

```text
usage: injector.py [-h]
                   [--output {kafka,print}]
                   [--bootstrap-servers BOOTSTRAP_SERVERS]
                   [--schema-registry-url SCHEMA_REGISTRY_URL]
                   [--topic-name TOPIC_NAME]
                   [--avg-qps AVG_QPS]
                   [--robot-probability ROBOT_PROBABILITY]
                   [--num-live-teams NUM_LIVE_TEAMS]
                   [--min-members-per-team MIN_MEMBERS_PER_TEAM]
                   [--max-members-per-team MAX_MEMBERS_PER_TEAM]
                   [--max-score MAX_SCORE]
                   [--late-event-rate LATE_EVENT_RATE]

options:
  -h, --help                      Show help message and exit.
  --output                        Output destination: 'kafka' or 'print'.
  --bootstrap-servers             Kafka bootstrap servers.
  --schema-registry-url           URL of the Schema Registry.
  --topic-name                    Kafka topic to publish to.
  --avg-qps                       Average events per second.
  --robot-probability             1-in-N chance of a team having a bot player.
  --num-live-teams                Number of active teams at a time.
  --min-members-per-team          Minimum team size.
  --max-members-per-team          Maximum team size.
  --max-score                     Maximum possible score in one event.
  --late-event-rate               1-in-N chance of event being late.
```
