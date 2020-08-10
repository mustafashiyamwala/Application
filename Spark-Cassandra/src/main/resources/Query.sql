CREATE KEYSPACE excelsior
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};

CREATE TYPE BBVars (
                  mt_aiflo double,
                  mt_aitmp double,
                  mt_aiden double
                
              );

CREATE TABLE BBData (
              bbluuid UUID,
              beanclassname text,
              datehourbucket text,
              fullyprocessed boolean,
              startblock boolean,
              ts text,
              vars frozen<BBVars>,
              PRIMARY KEY ((beanclassname, datehourbucket), ts)
          );