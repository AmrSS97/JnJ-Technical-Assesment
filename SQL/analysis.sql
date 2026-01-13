/** 

This single select statement fullfils all the needed 
requirnments in the second task of the technical assessment.

Note: COALESCE function used here to avoid breaking calculations if there are NULL values.
**/
SELECT
    SUM(COALESCE(cost_eur, 0)) AS total_maintenance_cost, -- calculating the sum of the maintenance costs for all the events --
    SUM(COALESCE(downtime_min, 0)) AS total_downtime_minutes, -- calculating the sum of all the downtimes --
    COUNT(*)                       AS total_maintenance_events, -- calculating the total number of events --
    SUM(CASE WHEN reason = 'Unplanned Breakdown' THEN 1 ELSE 0 END)
                                   AS unplanned_breakdowns, -- calculating the total number of unplanned breakdowns --
    AVG(downtime_min)              AS avg_downtime_per_event -- calculating the average of the downtime per event --
FROM maintenance_events
WHERE downtime_minutes IS NOT NULL;