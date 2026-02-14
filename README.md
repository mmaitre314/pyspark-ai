# PySpark AI

GitHub Copilot authoring PySpark notebooks running in VSCode Dev Container.

## QuickStart

- Open the Dev Container in VSCode.
- Generate synthetic data
  ```bash
  python generate_data.py --start-hour 2026-01-15-08 --end-hour 2026-01-21-23
  ```
- Open `water_billing.ipynb` in VSCode and run all cells.
- Run tests
  ```bash
  pytest test_water_billing.py -v
  ```

## References

- VSCode Dev Containers https://code.visualstudio.com/docs/devcontainers/containers
- Spark https://spark.apache.org/docs/latest/
- Local PySpark dev environment https://github.com/jplane/pyspark-devcontainer
