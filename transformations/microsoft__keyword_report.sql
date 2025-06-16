{% assign dimensions = vars.microsoft_advertising.models.microsoft__keyword_report.dimensions %}
{% assign metrics = vars.microsoft_advertising.models.microsoft__keyword_report.metrics %}
{% assign account_id = vars.microsoft_advertising.account_ids %}
{% assign active = vars.microsoft_advertising.active %}
{% assign table_active = vars.microsoft_advertising.models.microsoft__keyword_report.active %}
{% assign dataset_id = vars.output_dataset_id %}
{% assign table_id = vars.microsoft_advertising.models.microsoft__keyword_report.table_id %}
{% assign source_dataset_id = vars.microsoft_advertising.source_dataset_id %}
{% assign conversions = vars.microsoft_advertising.conversions %}
{% assign number_of_accounts = vars.microsoft_advertising.account_ids | size %}
{% assign has_pivots = vars.microsoft_advertising.pivots %}
{% assign source_table_id = 'keyword_performance_report' %}


CREATE OR REPLACE TABLE 
    `{{dataset_id}}`.`{{table_id}}` (
    {% for dimension in dimensions %}
        {% unless forloop.first %}
            , 
        {% endunless %}
        `{{dimension.name}}` {{dimension.type}} OPTIONS (description = '[db_field_name = {{dimension.name}}]') 
    {% endfor %}
    {% for metric in metrics %}
        , `{{metric.name}}` {{metric.type}}  OPTIONS (description = '[db_field_name = {{metric.name}}]') 
    {% endfor %}
    )
{% if active and table_active %}
    AS(
        with 
        
        sync_info as (
            select
              max(current_datetime()) as max_synced_at
              , max(date) as max_data_date
            from {{source_dataset_id}}.{{source_table_id}}
        )

        , campaigns as(
            select
                *
            from
                {{source_dataset_id}}.campaign_history
            where _gn_active = true
        )
        
        , accounts as(
            select
                *
            from
                {{source_dataset_id}}.account_history
            where _gn_active = true
        )
        
        , ad_groups as(
            select
                *
            from
                {{source_dataset_id}}.ad_group_history
            where _gn_active = true
        )
        
        , keywords as(
            select
                *
            from
                {{source_dataset_id}}.keyword_history
            where _gn_active = true
        )
        
        , report as(
            select
                date
                , campaign_id
                , ad_group_id
                , keyword_id
                , account_id
                , sum(spend) as cost
                , sum(clicks) as clicks
                , sum(impressions) as impressions
                , sum(conversions) as conversions
                , sum(revenue) as revenue
            from
                {{source_dataset_id}}.{{source_table_id}}
            group by
                1,2,3,4,5
        )

        {% if has_pivots %}
        , pivots as(
            select
                *
            from
                (
                    select
                        date
                        , account_id
                        , ad_group_id
                        , campaign_id
                        , keyword_id
                        , all_conversions -- This is what we are going to sum in our pivot table
                        , all_revenue  -- This is what we are going to sum in our pivot table
                        , view_through_conversions  -- This is what we are going to sum in our pivot table
                        , goal
                    from
                        {{source_dataset_id}}.goals_and_funnels_report
                )
            pivot(
                /* 
                    We need to toggle these. For instance we may not always want all of these columns by
                    default because it may be confusing for the user. Perhaps we only typically want 
                    all_conversions and all_conversions_value
                */
                sum(safe_cast(all_conversions as float64)) as all_conversions
                , sum(safe_cast(all_revenue as float64)) as all_revenue
                , sum(safe_cast(view_through_conversions as float64)) as view_through_conversions
                for goal in (
                -- Need to create a loop here for conversion action names and their aliases
                    {% for conversion in conversions %}
                        {% unless forloop.first %}
                            , 
                        {% endunless %}
                        "{{conversion.event_name}}" `{{ conversion.output_name }}`
                    {% endfor %}
                )
        
            )
        )
        {% endif %}

        , api as(
            select
                report.date
                , report.campaign_id
                , campaigns.name as campaign_name
                , report.ad_group_id
                , ad_groups.name as ad_group_name
                , accounts.id as account_id
                , accounts.name as account_name
                , keywords.text as keyword_text
                , keywords.match_type as keyword_match_type
                , report.clicks
                , report.cost
                , report.impressions
                , report.conversions
                , report.revenue
                , sync_info.max_synced_at as last_synced_at
                , sync_info.max_data_date as last_data_date
                {% if has_pivots %}
                    {% for conversion in conversions %}
                        /* 
                            We need to toggle these. For instance we may not always want all of these columns by
                            default because it may be confusing for the user. Perhaps we only typically want 
                            all_conversions and all_conversions_value
                        */
                        , pivots.all_conversions_{{conversion.output_name}}
                        , pivots.all_revenue_{{conversion.output_name}}
                        , pivots.view_through_conversions_{{conversion.output_name}}
                    {% endfor %}
                {% endif %}
            from
                report
            {% if has_pivots %}
                left join
                    pivots
                on
                    report.date = pivots.date
                and
                    report.account_id = pivots.account_id
                and
                    report.ad_group_id = pivots.ad_group_id
                and
                    report.campaign_id = pivots.campaign_id
                and
                    report.keyword_id = pivots.keyword_id
            {% endif %}
            left join
                campaigns
            on
                report.campaign_id = campaigns.id
            left join
                ad_groups
            on
                report.ad_group_id = ad_groups.id
            left join
                accounts
            on
                report.account_id = accounts.id
            left join
                    keywords
                on
                    report.keyword_id = keywords.id
            left join
                sync_info
            on
                true
        )
    
        select
        {% for dimension in dimensions %}
            {% unless forloop.first %}
                , 
            {% endunless %}
            CAST({{dimension.expression}} as {{dimension.type}}) as `{{dimension.name}}`
        {% endfor %}
        {% for metric in metrics %}
            , CAST({{metric.expression}} as {{metric.type}}) as `{{metric.name}}`
        {% endfor %}
    from
        api
        {% if number_of_accounts > 0 %}
            where account_id in(
                {% for id in account_id %}
                    {% unless forloop.first %}
                        , 
                    {% endunless %}
                    {{id}}
                {% endfor %}
            )
        {% endif %}
    group by
        {% for dimension in dimensions %}
            {% unless forloop.first %}
                , 
            {% endunless %}
            {{forloop.index}}
        {% endfor %}
)
{% endif %}
;