#!/usr/bin/env bash
#
# Runs the benchmark in a dedicated, bare JVM — NOT through `mvn exec:java`.
#
# Why: Maven's JVM carries the Maven core, plugins and their dependencies (guava,
# plexus, ...). That inflates heap/metaspace and peak RSS — and peak RSS is exactly
# one of the numbers the harness records (docs/memory.md). It also adds startup and
# classloading latency before the first measurement. So: Maven builds the classpath,
# `java` runs the benchmark. The only JVM overhead present is the benchmark itself.
#
# The dataset shape (messages x remittances-per-message x transactions-per-remittance)
# is mandatory. Usage examples:
#   benchmark/run-benchmark.sh --messages 4 --remittances-per-message 10 \
#       --transactions-per-remittance 2500 --out bench/results.json
#   benchmark/run-benchmark.sh --messages 1 --remittances-per-message 80000 \
#       --transactions-per-remittance 25 --out bench/results-2m.json
#
# Extra JVM options (heap sizing etc.) can be passed via DFE_JVM_OPTS:
#   DFE_JVM_OPTS="-Xmx16g" benchmark/run-benchmark.sh --messages 1 --remittances-per-message 80000
set -euo pipefail
cd "$(dirname "$0")/.."

NATIVE_LIB="rust/target/release/libdatafusion_java_bridge.so"

# 1. Native library (build only if missing).
if [ ! -f "$NATIVE_LIB" ]; then
    echo ">> building native library (cargo build --release)"
    (cd rust && cargo build --release)
fi

# 2. Compile + assemble the runtime classpath with Maven (build-time only).
echo ">> assembling classpath via Maven (not used at benchmark runtime)"
mvn -q -pl java test-compile
mvn -q -pl java dependency:copy-dependencies \
    -DincludeScope=runtime \
    -DoutputDirectory="$PWD/java/target/bench-libs" \
    -DskipTests=true

# 3. Launch the benchmark in a clean JVM: no Maven, no plugin classloaders.
java \
    --enable-native-access=ALL-UNNAMED \
    --add-opens=java.base/java.nio=ALL-UNNAMED \
    -Dio.netty.tryReflectionSetAccessible=true \
    ${DFE_JVM_OPTS:-} \
    -cp "java/target/classes:java/target/test-classes:java/target/bench-libs/*" \
    io.github.balchua.datafusion.bench.BenchmarkMain "$@"

echo ">> done (results: see --out paths above; CSV is written next to the JSON)"