pushd src/cmd/syslog-agent
git reset --hard origin/https_retry
git pull
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build

DEPLOYMENT=cf_cells_1
BINARY=syslog-agent

upload_binary() {
    VM_ID=$1
    echo "uploading to: $VM_ID"

    bosh -d $DEPLOYMENT ssh $VM_ID "if [ -f /tmp/$BINARY ]; then sudo rm /tmp/$BINARY; fi"
    bosh -d $DEPLOYMENT scp $BINARY $VM_ID:/tmp/$BINARY
    bosh -d $DEPLOYMENT ssh $VM_ID '
        sudo monit stop loggr-syslog-agent && \
        sleep 2 && \
        sudo rm -f /var/vcap/sys/log/loggr-syslog-agent/loggr-syslog-agent.stderr.log && \
        sudo rm -f /var/vcap/sys/log/loggr-syslog-agent/loggr-syslog-agent.stdout.log && \
        sudo cp /var/vcap/packages/syslog-agent/syslog-agent /var/vcap/packages/syslog-agent/syslog-agent_old && \
        sudo cp /tmp/syslog-agent /var/vcap/packages/syslog-agent/syslog-agent && \
        sudo chmod 755 /var/vcap/packages/syslog-agent/syslog-agent && \
        sudo monit start loggr-syslog-agent
    '
}

for i in {0..5}; do
    upload_binary "diego-cell/$i" &
done

wait
echo "All uploads finished."