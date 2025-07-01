# Day 1 - Lab

In this lab, we create a pretend runbook for EcZachly Inc Growth Pipeline.

---

# On call runbook for EcZachly Inc Growth Pipeline

Primary Owner: Zach

Secondary Owner: Lulu

## Common Issues

**Upstream Datasets**

- Website events
    - Common anomalies
        - Sometimes referrer is NULL too much, this is fixed downstream but we are alerted about it because it messes with the metrics *[Non blocking DQ check — Ed]*
- User exports
    - Export might fail to be extracted on a given day. When this happens just use yesterday’s export for today.

**Downstream consumers**

- Experimentation platform
- Dashboards

## SLAs

The data should land 4 hours after UTC midnight

---

Obviously, this runbook is the ugliest thing ever, but the idea here is that now, if someone goes to this pipeline, they have a document they can refer to.