set -o errexit
set -o pipefail
set -o nounset
shopt -s nullglob

export PATH=/src/gorgon_couchbase:$PATH

wait_for_node() {
    until nc -q 1 "$1" "$2" < /dev/null ; do sleep 1 ; done
}

echo "Nodes: $NODES"
for node in ${NODES//,/ } ; do
    wait_for_node $node 9090
done

{
    failed=0
    for workload in /workloads/*.sh ; do
        echo
        echo "Running $workload"
        echo
        bash "$workload" || failed=1
    done
    exit $failed
} 2>&1 | tee gorgon.log
exit_code=${PIPESTATUS[0]}

tar -czf files.tgz gorgon.log *.html

gorgon_couchbase -gorgon-nodes $NODES closerpc

echo
echo DONE

exit $exit_code
