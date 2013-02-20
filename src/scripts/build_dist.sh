#!/bin/sh

if [ ! -e "src/main/python/rest/__init__.py" ] ; then
  echo "This script must be run from the base project dir!"
  exit 1
fi

if [ $# -lt 1 ] ; then
  echo "usage: $0 <rel_version>"
  exit 1
fi

set -e

REL_VERSION="$1"
ZIP_DIR="target/app-engine-rest-server"

rm -rf target

mkdir -p "${ZIP_DIR}"
mkdir -p "${ZIP_DIR}/rest"


cp LICENSE *.txt "${ZIP_DIR}"

cp "src/main/python/rest/__init__.py" "${ZIP_DIR}/rest"

cd "target"

zip -q -r "app-engine-rest-server-${REL_VERSION}.zip" "app-engine-rest-server"

echo "Built target/app-engine-rest-server-${REL_VERSION}.zip"
