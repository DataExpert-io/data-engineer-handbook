# Data Engineering team members: William, Dong, Siddhant, Maxime

## Runbook for Unit Profit Pipeline
Primary Owner: William
Secondary Owner: Dong

Upstream Owners: Finance Team
Downstream Owners: Business Analytics Team

Data Processed: 
1. Gross and net sales for individual accounts
2. Gross and net expenditure for individual accounts

Common Issues:
1. Upstream datasets - sometimes refunds might throw an anomaly NULL user

SLAs: 
Latency of 24 hours after midnight GMT+0/BST
Issue escalation response time: 12 hours

## Runbook for Agg Profit Pipeline
Primary Owner: Maxime
Secondary Owner: Siddhant

Upstream Owners: Finance Team
Downstream Owners: Reporting

Data Processed: 
1. Gross and net sales for aggregated accounts
2. Gross and net expenditure for aggregated accounts

Common Issues: No regular occuring issues

SLAs: 
Latency of 8 hours after midnight GMT+0/BST
Issue escalation response time: 4 hours

## Runbook for Daily Growth Pipeline
Primary Owner: Siddhant
Secondary Owner: Dong

Upstream Owners: User Accounts Team
Downstream Owners: Business Analytics Team

Data Processed: 
1. Individual user accounts information

Common Issues:
1. Upstream datasets - sometimes user count might be off if there are delays in pipeline and will affect calculations

SLAs: 
Latency of 24 hours after midnight GMT+0/BST
Issue escalation response time: 12 hours

## Runbook for Agg Growth Pipeline
Primary Owner: William
Secondary Owner: Maxime

Upstream Owners: User Accounts Team
Downstream Owners: Reporting

Data Processed: 
1. Aggregated user accounts information

Common Issues: No regular occuring issues

SLAs: 
Latency of 8 hours after midnight GMT+0/BST
Issue escalation response time: 4 hours

## Runbook for Agg Engagement Pipeline
Primary Owner: Dong
Secondary Owner: William

Upstream Owners: Software Engineering Team
Downstream Owners: Business Analytics Team

Data Processed: 
1. Individual user accounts information
2. Platform events

Common Issues:
1. OOM issue from skew - perform salting
2. Kafka operation might affect availability of data - allocate sufficient resources to Kafka
3. Some data might fall through the cracks and not be properly deduplicated

SLAs: 
Latency of 8 hours after midnight GMT+0/BST
Issue escalation response time: 4 hours

## Oncall Schedule
Weekly cycle rotation (1 week/month) - can swap to accomodate for holidays

Cycle between A -> B -> C -> D -> A -> .....

If staff B plans to take holidays during their oncall week, they can swap with any other staff provided they mutually agreed to swap. For example if B swaps with D:

A -> D -> C -> B -> A -> B -> .....