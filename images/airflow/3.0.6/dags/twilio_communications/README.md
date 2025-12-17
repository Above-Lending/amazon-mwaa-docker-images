# Twilio

## Overview

### DAG: `twilio_reverse_number_lookups`
This DAG performs reverse phone number lookups using Twilio's Lookup API to retrieve caller information, carrier details, and phone number validation data.

#### What if this DAG fails?

**Rerun as soon as you can.**

This DAG looks up phone numbers in the `above_dw_prod.twilio.reverse_number_lookups` which are stale (i.e., their `_last_lookup` value is older than a certain amount, currently 12 months), hits the Twilio API, then merges the new record into the `reverse_number_lookups` table.  Rerunning the DAG will run through the stale numbers again and update them.

#### Can I run this locally?

**Yes, but please limit the number of API calls you do**.  This will hit the production API, but it will not merge into any production tables.

## Runbook

TODO.