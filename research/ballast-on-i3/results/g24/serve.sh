#!/bin/bash
# our server, product defaults (sealed runs 2, compactor, derived documents, T_pin 30 s), the round's layout options
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
OPTS="--envelopes write --customer-key messaggi=cliente:ts,eventi=cliente:ts,ordini=cliente:ts --replica-set rs0 --listen 127.0.0.1:27017 ${EXTRA:-}"
echo "$OPTS" > /home/user/work/g24/server-options.txt
exec /home/user/fenvix-engine/target/release/mongo-server --store s3 --bucket ballast24 --region us-east-1 --endpoint http://127.0.0.1:9000 $OPTS
