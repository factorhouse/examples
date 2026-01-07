docker clone https://github.com/factorhouse/examples.git
cd examples
docker clone -b feature/data-governance https://github.com/factorhouse/factorhouse-local.git
## check sources
# $ pwd
# /home/jaehyeon/factorhouse/examples
# $ ls projects/data-governance-lab/
# datagen  note-pg-error.sh  note.sh
# $ ls factorhouse-local/
# LICENSE  README.md  SECURITY.md  compose-flex.yml  compose-kpow.yml  compose-metadata.yml  compose-obsv.yml  compose-store.yml  images  resources

export ST_LICENSE_PREFIX="community"
export ST_LICENSE=/home/jaehyeon/.license/shadowtraffic/$ST_LICENSE_PREFIX-license.env

USE_EXT=false docker compose -p flex -f ./factorhouse-local/compose-flex.yml up -d postgres

docker run --rm \
  --env-file $ST_LICENSE \
  --network factorhouse \
  -v ./projects/data-governance-lab/datagen/shadowtraffic-tmp.json:/home/config.json \
  shadowtraffic/shadowtraffic:latest \
  --config /home/config.json


USE_EXT=false docker compose -p flex -f ./factorhouse-local/compose-flex.yml down

## error
#   Detail: Key (user_id)=(a5266917-e404-1506-6a68-42ac3acd96e7) already exists.  Call getNextException to see other errors in the batch.
#         at org.postgresql.jdbc.BatchResultHandler.handleCompletion(BatchResultHandler.java:186)
#         at org.postgresql.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:591)
#         at org.postgresql.jdbc.PgStatement.internalExecuteBatch(PgStatement.java:889)
#         at org.postgresql.jdbc.PgStatement.executeBatch(PgStatement.java:913)
#         at org.postgresql.jdbc.PgPreparedStatement.executeBatch(PgPreparedStatement.java:1739)
#         at next.jdbc$execute_batch_BANG_$fn__16628.invoke(jdbc.clj:327)
#         at clojure.core$map$fn__5931$fn__5932.invoke(core.clj:2759)
#         at clojure.lang.PersistentVector.reduce(PersistentVector.java:343)
#         at clojure.core$transduce.invokeStatic(core.clj:6947)
#         at clojure.core$into.invokeStatic(core.clj:6963)
#         at clojure.core$into.invoke(core.clj:6951)
#         at next.jdbc$execute_batch_BANG_.invokeStatic(jdbc.clj:322)
#         at next.jdbc$execute_batch_BANG_.invoke(jdbc.clj:276)
#         at next.jdbc$execute_batch_BANG_.invokeStatic(jdbc.clj:339)
#         at next.jdbc$execute_batch_BANG_.invoke(jdbc.clj:276)
#         at next.jdbc.sql$insert_multi_BANG_.invokeStatic(sql.clj:88)
#         at next.jdbc.sql$insert_multi_BANG_.invoke(sql.clj:45)
#         at next.jdbc.sql$insert_multi_BANG_.invokeStatic(sql.clj:80)
#         at next.jdbc.sql$insert_multi_BANG_.invoke(sql.clj:45)
#         at io.shadowtraffic.connections.postgres$insert_BANG_.invokeStatic(postgres.clj:17)
#         at io.shadowtraffic.connections.postgres$insert_BANG_.invoke(postgres.clj:13)
#         at io.shadowtraffic.connections.postgres$execute_op_BANG_.invokeStatic(postgres.clj:31)
#         at io.shadowtraffic.connections.postgres$execute_op_BANG_.invoke(postgres.clj:29)
#         at io.shadowtraffic.connections.postgres$write_batch.invokeStatic(postgres.clj:39)
#         at io.shadowtraffic.connections.postgres$write_batch.invoke(postgres.clj:36)
#         at clojure.core$partial$fn__5908.invoke(core.clj:2641)
#         at io.shadowtraffic.connections.async$buffered_async_reader$reify__14234.get(async.clj:95)
#         at java.base/java.util.concurrent.CompletableFuture$AsyncSupply.run(CompletableFuture.java:1812)
#         at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
#         at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
#         at java.base/java.lang.Thread.run(Thread.java:1575)
# Caused by: org.postgresql.util.PSQLException: ERROR: duplicate key value violates unique constraint "orders_pkey"
#   Detail: Key (user_id)=(a5266917-e404-1506-6a68-42ac3acd96e7) already exists.
#         at org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2733)
#         at org.postgresql.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:2420)
#         at org.postgresql.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:580)
#         ... 29 more