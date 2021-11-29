#!/bin/sh

WORK_HOME="/svc/nudge/policy_nudge"

echo $WORK_HOME

cd $WORK_HOME

echo "POLICY_NUDGE_START"

python $WORK_HOME/src/policy_nudge.py 0

echo "POLICY_NUDGE_END"
