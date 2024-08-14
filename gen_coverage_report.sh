#!/bin/bash
mkdir -p coverage
COVERAGE_FILE=coverage/info
REPORT_FOLDER=coverage/report
lcov --rc lcov_branch_coverage=1 -c -d build -o ${COVERAGE_FILE}_tmp
lcov --rc lcov_branch_coverage=1  -e ${COVERAGE_FILE}_tmp "*cpp" "*h" -o ${COVERAGE_FILE}
genhtml --rc genhtml_branch_coverage=1 ${COVERAGE_FILE} -o ${REPORT_FOLDER}
