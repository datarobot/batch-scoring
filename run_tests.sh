#!/bin/bash

CDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DATAROBOT_DIR="${DATAROBOT_DIR}"

export ENABLE_FULL_DATASET_PARTITIONING_AND_SAMPLING=0

echo 'Starting prediction negative cases tests...'

# remove stale LXC context name
sudo rm -R "${CDIR}/tests/testworkspacetemp/sw-pid-uid"

function start_server() {
    "${DATAROBOT_DIR}/start.sh"
    if [ $? -ne 0 ]; then
        exit 1
    fi
}

PYTEST_PARAMS="-v"

function run_tests() {
    if [ -n "$JENKINS_URL" ]; then
        set -x # Prints commands as they are executed
        export PYTHONPATH="`pwd`"

        py.test ${PYTEST_PARAMS} --junitxml=./testResults.xml --cov-report xml --cov ./ "${CDIR}/tests/test_functional.py"
    else
        py.test ${PYTEST_PARAMS} --assert=plain $* "${CDIR}/tests/"
    fi
}

function stop_server() {
    "${DATAROBOT_DIR}/stop.sh"
}

while test $# -gt 0; do
    case "$1" in
        -h|--help)
            echo "Prediction negative cases tests runner."
            echo "Usage: ./run_prediction_negative_cases.sh [-d|--debug] [-s|--skip-start-stop] [-f|--fail-fast] [-k <TAG>]"
            echo "-d|--debug              show print statements"
            echo "-s|--skip-start-stop    do not start/stop DataRobot application"
            echo "-f|--fail-fast          fail after forst test"
            echo "-k <TAG>                run only tests which matches the tag"
            exit 0
            ;;
        -s|--skip-start-stop)
            SKIP_START_STOP=true
            shift
            ;;
        -d|--debug)
            PYTEST_PARAMS+=" -s"
            shift
            ;;
        -f|--fail-fast)
            PYTEST_PARAMS+=" -x"
            shift
            ;;
        -k)
            shift
            if test $# -gt 0; then
                PYTEST_PARAMS+=" -k $1"
            else
                echo "Test tag is not specified but -k found"
                exit 1
            fi
            shift
            ;;
        *)
            break
            ;;
    esac
done

if [ "${SKIP_START_STOP}" = "true" ]; then
    run_tests
else
    start_server
    run_tests
    stop_server
fi
