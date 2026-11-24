for PRESET in small medium large; do
    echo "========== $PRESET =========="

    cd generator
    python generate.py --preset "$PRESET" --seed 42 --out "out/$PRESET"
    python load.py "out/$PRESET" --truncate
    cd ..

    cd benchmark
    python run_benchmark.py \
        --preset "$PRESET" \
        --sql --spark \
        --repeats 3 \
        --seed 42 \
        --notes "full $PRESET"
    cd ..
done
