# Getting Started

The fastest way to try **Kpow** (Kafka) and/or **Flex** (Flink) locally. 

Each folder is a self-contained `docker-compose.yml`. You'll need to get a free license, but once you've included it in its docker file, that's all the setup you'll need. When you've got the docker file running, you'll be able to see it on localhost. It is worth noting that will take a couple of minutes for it to warm up and for data to appear.

If you're looking for the full local development environment (multi-broker Kafka, Flink + Spark + Iceberg + Pinot, etc.) instead, see [Factor House Local](https://github.com/factorhouse/factorhouse-local). And if you want to explore more, try [the Labs](https://github.com/factorhouse//fh-local-labs/).

## Prerequisites

- Docker (Desktop or Engine) installed and running.
- A Factor House license. Get one free at [account.factorhouse.io/](https://account.factorhouse.io/) — a single **Community License** (free, non-commercial) covers a limited set of features for Kpow and Flex. A 30-day trial license gives full Enterprise features for Kpow or Flex if you want to evaluate those instead.

## Pick a path

| Folder | What it starts | Where |
|---|---|---|
| [`kpow/`](./kpow/) | Kpow (Enterprise) + a single Kafka broker (KRaft) + a producer that seeds a topic for you | `cd getting-started/kpow` |
| [`flex/`](./flex/) | Flex (Enterprise) + a standalone Flink cluster running the built-in `TopSpeedWindowing` example job | `cd getting-started/flex` |
| [`community/`](./community/) | Kpow and/or Flex **Community Edition**, same idea, selected via Compose profiles | `cd getting-started/community` |

### Add your license

Each `docker-compose.yml` here has a `LICENSE_ID` / `LICENSE_CODE` / `LICENSEE` / `LICENSE_EXPIRY` / `LICENSE_SIGNATURE` block with either `<PLACEHOLDER>` values or nothing filled in. Open the license file you generated from the portal above and copy the matching values into that block before running `docker compose up`.

These are meant to stay **local, uncommitted changes** — don't commit a compose file with real license values filled in.

### Run the docker command

```bash
# in the kpow or flex folder
docker compose up -d
```
Kpow or Flex: `http://localhost:3000`

```bash
# in the community folder (add --profile kpow, --profile flex or both)
docker compose --profile kpow --profile flex up -d
```
Kpow: `http://localhost:3000` · Flex: `http://localhost:3001`

## Stop everything

From inside whichever folder you started:

```bash
docker compose down
```

(for `community/`, add the same `--profile` flags you used to start it)
