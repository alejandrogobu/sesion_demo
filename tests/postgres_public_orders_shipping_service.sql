SELECT *
FROM 
    {{ source('postgres_public','orders') }}
WHERE 
    shipping_service = '' 
    AND tracking_id <> ''