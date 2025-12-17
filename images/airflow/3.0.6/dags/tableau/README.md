# Tableau

## Overview

Used to backup and take snapshots of our Tableau Workbooks and Custom queries.

### DAG: `tableau_backup`

Creates a backup of workbooks.  Will only back up an item if it has changed since the last backup.

#### What if this DAG fails?

Rerun the DAG as soon as you can to make sure the backups are up-to-date.  Backups are rarely used so this is not a critical failure.

#### Can I run this locally?

**TODO**.


### DAG: `tableau_custom_query_backup``

Creates a backup of custom queries.  Will only back up an item if it has changed since the last backup.

#### What if this DAG fails?

**TODO**

#### Can I run this locally?
**TODO**

### DAG: `tableau_snapshot`

Takes a snapshot of workbooks.  Snapshots are taken on a regular schedule regardless of whether the workbook has changed.

#### What if this DAG fails?

You should rerun the DAG as soon as possible to ensure that snapshots are taken on schedule.  Snapshots are used for auditing and historical purposes.

#### Can I run this locally?
**TODO**

## Runbook

TODO.