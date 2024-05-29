WITH message_summary AS (
    SELECT 
        cm."orderId" AS order_id,
        MIN(cm."messageSentTime") AS conversation_started_at,
        MAX(cm."messageSentTime") AS last_message_time,
        CASE WHEN MAX(CASE WHEN cm."resolution" = TRUE THEN 1 ELSE 0 END) = 1 THEN TRUE ELSE FALSE END AS resolved,
        MIN(CASE WHEN cm."fromId" = cm."agentId" THEN cm."messageSentTime" ELSE NULL END) AS first_agent_message,
        MIN(CASE WHEN cm."fromId" = cm."customerId" THEN cm."messageSentTime" ELSE NULL END) AS first_customer_message,
        COUNT(CASE WHEN cm."fromId" = cm."agentId" THEN 1 ELSE NULL END) AS num_messages_agent,
        COUNT(CASE WHEN cm."fromId" = cm."customerId" THEN 1 ELSE NULL END) AS num_messages_customer,
        MIN(CASE WHEN cm."fromId" = cm."customerId" THEN cm."messageSentTime" ELSE NULL END) < 
        MIN(CASE WHEN cm."fromId" = cm."agentId" THEN cm."messageSentTime" ELSE NULL END) AS first_message_by_customer
    FROM processed_data.customer_agent_messages cm
    GROUP BY cm."orderId", cm."customerId", cm."agentId"
),
response_time AS (
    SELECT 
        order_id,
        EXTRACT(EPOCH FROM (first_agent_message - conversation_started_at)) AS first_response_time_delay_seconds
    FROM message_summary
    WHERE first_agent_message IS NOT NULL
),
last_message_stage AS (
    SELECT
        o."orderId" AS order_id,
        o."cityCode" AS city_code,
        MAX(CASE WHEN cm."priority" = TRUE THEN 1 ELSE 0 END) AS last_message_order_stage
    FROM processed_data.orders o
    JOIN processed_data.customer_agent_messages cm ON o."orderId" = cm."orderId"
    GROUP BY o."orderId", o."cityCode"
)
INSERT INTO transformed_data.customer_agent_conversations (
    order_id, city_code, first_agent_message, first_customer_message,
    num_messages_agent, num_messages_customer, first_message_by,
    conversation_started_at, first_response_time_delay_seconds,
    last_message_time, last_message_order_stage, resolved
)
SELECT
    ms.order_id,
    lms.city_code,
    ms.first_agent_message,
    ms.first_customer_message,
    ms.num_messages_agent,
    ms.num_messages_customer,
    CASE WHEN ms.first_message_by_customer THEN 'customer' ELSE 'agent' END AS first_message_by,
    ms.conversation_started_at,
    rt.first_response_time_delay_seconds,
    ms.last_message_time,
    lms.last_message_order_stage,
    ms.resolved
FROM
    message_summary ms
LEFT JOIN
    response_time rt ON ms.order_id = rt.order_id
LEFT JOIN
    last_message_stage lms ON ms.order_id = lms.order_id;

