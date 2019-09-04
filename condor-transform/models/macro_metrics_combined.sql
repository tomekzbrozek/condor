SELECT
    gold.date AS date,
    gold.gold_price_usd AS gold_price_usd,
    inx.inx_close_value AS inx_close_value,
    effr.effr_rate AS effr_rate
FROM
    `peppy-bond-105017.macroeconomic.gold` AS gold
LEFT JOIN
    `peppy-bond-105017.macroeconomic.inx` AS inx
    ON gold.date = inx.date
LEFT JOIN
    `peppy-bond-105017.macroeconomic.effr` AS effr
    ON effr.date = inx.date
