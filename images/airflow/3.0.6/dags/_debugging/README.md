# Debugging

## Overview

### DAG: `list_python_packages`

This DAG lists all installed Python packages in the MWAA Airflow environment.

#### What if this DAG fails?

This DAG should not fail on a healthy environment. If it does, check the Airflow logs for errors related to package listing.

#### Can I run this locally?

Yes, it will give you local environment information.
