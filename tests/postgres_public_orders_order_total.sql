SELECT 
*
FROM 
    {{ source('postgres_public','orders') }} O
    LEFT JOIN {{ source('postgres_public','promos') }} P on O.promo_id = P.promo_id
WHERE 
    ROUND((order_cost + shipping_cost - coalesce(discount,0)),2) != order_total