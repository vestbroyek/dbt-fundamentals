-- Practice using dbt_utils
-- List every day in 2020
{{ dbt_utils.date_spine(
    datepart = "day",
    start_date = "cast('01/01/2020' as date)",
    end_date = "cast(31/12/2020 as date)"
    )
}}