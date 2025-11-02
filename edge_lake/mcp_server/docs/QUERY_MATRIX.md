# Query Processing Matrix


| NODE     | Process    | Pass<br>Thr |BLOCK| row<br> by row | output<br>>manager | deliver<br>>rows | query<br>>local db | Select<br>>Parser | process<br>query |
|----------|------------|-------------|-----|----------------|--------------------|------------------|--------------------|-------------------|------------------|
| OPERATOR | Local DB   | YES         | NO  |                |                    |                  | YES - OP NODE      |                   |                  |
| OPERATOR | Local DB   | YES         | YES | YES - OP NODE  |                    |                  | YES - OP NODE      |                   |                  |
| QUERY    | Remote DB  | YES         | NO  |                |                    |                  |                    |                   |                  |
| QUERY    | Remote DB  | YES         | YES | YES - OP NODE  |                    |                  |                    |                   |                  |
| QUERY    | Network DB | YES         | NO  |                |                    |                  |                    |                   |                  |
| QUERY    | Network DB | YES         | YES | YES - Q NODE   |                    |                  |                    |                   |                  |
| QUERY    | Network DB | NO          | NO  |                |                    |                  | YES - Q NODE       |                   |                  |
| QUERY    | Network DB | NO          | YES | YES - Q NODE   |                    |                  | YES - Q NODE       |                   |                  |
