/* 
    Change these variables so that the <CONNECTOR SERVICE> portion matches the connector service
    that you are working with. For example if you are creating a model for google_ads then replace the text
    and <> signs with google_ads
    
    Change these variables so that the <MODEL NAME> portion matches the model name that you are working with
    If you are creating a model called ad_report__google, you will change the <MODEL NAME> with ad_report__google
*/
{% assign dimensions = vars.facebook_ads.models.facebook__platform_report.dimensions %}
{% assign metrics = vars.facebook_ads.models.facebook__platform_report.metrics %}
{% assign account_id = vars.facebook_ads.account_ids %}
{% assign active = vars.facebook_ads.active %}
{% assign table_active = vars.facebook_ads.models.facebook__platform_report.active %}
{% assign dataset_id = vars.output_dataset_id %}
{% assign table_id = vars.facebook_ads.models.facebook__platform_report.table_id %}
-- {% assign source_dataset_id = vars.facebook_ads.source_dataset_id %}
{% assign source_dataset_id = 'organization_10_account_282_loud_coahse_account_facebook_client_bombastic_bedground' %}
{% assign conversions = vars.facebook_ads.conversions %}
{% assign number_of_accounts = vars.facebook_ads.account_ids | size %}
{% assign regexp_filters = vars.facebook_ads.regexp_filters %}
{% assign has_actions = vars.facebook_ads.actions %}
{% assign has_action_values = vars.facebook_ads.action_values %}
-- {% assign source_table_id = 'platform_stats' %}
{% assign source_table_id = 'platform_report' %}

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
        /* 
            Add as many CTEs here as needed to keep the code readable. 
        */
        sync_info as (
            select
              max(current_datetime()) as max_synced_at
            --   max(datetime(_fivetran_synced, "{{ vars.timezone }}")) as max_synced_at
              , max(date) as max_data_date
            from {{source_dataset_id}}.{{source_table_id}}
        )

        , campaigns as(
            select
                *
            from
               {{source_dataset_id}}.campaign_history
            where is_current = true
            -- qualify rank() over(partition by id order by updated_time desc) = 1
            -- and row_number() over(partition by id, updated_time) = 1
        )
        
        , accounts as(
            select
                *
            from
                {{source_dataset_id}}.account_history
            where
                is_current = true
            -- qualify rank() over(partition by id order by _fivetran_synced desc) = 1
            -- and row_number() over(partition by id, _fivetran_synced) = 1
        )
        
        , ad_sets as(
            select
                *
            from
               {{source_dataset_id}}.ad_set_history
            where is_current = true
            -- qualify rank() over(partition by id order by updated_time desc) = 1
            -- and row_number() over(partition by id, updated_time) = 1
        )
        
        , ads as(
            select
                *
            from
               {{source_dataset_id}}.ad_history
            where is_current = true
            -- qualify rank() over(partition by id order by updated_time desc) = 1
            -- and row_number() over(partition by id, updated_time) = 1
        )
        
        , report as(
            select
                date
                -- , _fivetran_id
                , ad_id
                , publisher_platform
                , sum(spend) as cost
                , sum(clicks) as clicks
                , sum(impressions) as impressions
                , sum(0) as frequency
                , sum(0) as reach
                , 0 as conversions
            from
               {{source_dataset_id}}.{{source_table_id}}
            group by
                1,2,3
                -- 1,2,3,4
        )
        
        {% if has_actions %}
            , actions as(
                select
                    *
                from
                    (select ad_id, publisher_platform, date, action_type, value from {{source_dataset_id}}.{{source_table_id}}_actions)
                pivot(sum(value) as actions FOR action_type in (
                    {% for conversion in conversions %}
                        {% if forloop.first %}
                            '{{conversion.event_name}}' as {{conversion.output_name}}
                        {% else %}
                            , '{{conversion.event_name}}' as {{conversion.output_name}}
                        {% endif %}
                    {% endfor %}
                    )
                )
            )
        {% endif %}
        
        {% if has_action_values %}
            , action_values as(
                select
                    *
                from
                    (select ad_id, publisher_platform, date, action_type, value from {{source_dataset_id}}.{{source_table_id}}_action_values)
                pivot(sum(value) as action_values FOR action_type in (
                    {% for conversion in conversions %}
                        {% if forloop.first %}
                            '{{conversion.event_name}}' as {{conversion.output_name}}
                        {% else %}
                            , '{{conversion.event_name}}' as {{conversion.output_name}}
                        {% endif %}
                    {% endfor %}
                    )
                )
            )
        {% endif %}
        
        , api as(
            select
                report.* except(ad_id, publisher_platform)
                , case
                    when report.publisher_platform = 'facebook' then 'Facebook'
                    when report.publisher_platform = 'instagram' then 'Instagram'
                    when report.publisher_platform = 'messenger' then 'Messenger'
                    when report.publisher_platform = 'audience_network' then 'Audience Network'
                    when report.publisher_platform = 'unknown' then 'Unknown'
                    else INITCAP(report.publisher_platform)
                end as platform
                , sync_info.max_synced_at as last_synced_at
                , sync_info.max_data_date as last_data_date
                , {{has_actions}} as has_actions
                , {{has_action_values}} as has_action_values
                , report.ad_id
                , ads.name as ad_name
                , ad_sets.id as ad_set_id
                , ad_sets.name as ad_set_name
                , campaigns.id as campaign_id
                , campaigns.name as campaign_name
                , accounts.account_id as account_id
                , accounts.name as account_name
                {% if has_actions %}
                    , actions.* except(ad_id, date)
                {% endif %}
                {% if has_action_values %}
                    , action_values.* except(ad_id, date)
                {% endif %}
                
            from
                report
            left join
                ads
            on
                safe_cast(report.ad_id as string) = safe_cast(ads.id as string)
            left join
                ad_sets
            on
                -- ads.ad_set_id = ad_sets.id
                safe_cast(ads.ad_set_id as string) = safe_cast(ad_sets.id as string)
            left join
                campaigns
            on
                safe_cast(ad_sets.campaign_id as string) = safe_cast(campaigns.id as string)
            left join
                accounts
            on
                safe_cast(campaigns.account_id as string) = safe_cast(accounts.account_id as string)
            {% if has_actions %}
                left join
                    actions
                on
                    report.date = actions.date
                and
                    safe_cast(report.ad_id as string) = safe_cast(actions.ad_id as string)
                and
                    report.publisher_platform = actions.publisher_platform
            {% endif %}
            {% if has_action_values %}
                left join
                    action_values
                on
                    report.date = action_values.date
                and
                    safe_cast(report.ad_id as string) = safe_cast(action_values.ad_id as string)
                and
                    report.publisher_platform = action_values.publisher_platform
            {% endif %}
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
            {% if regexp_filters != blank and regexp_filters != false %}
                {% if number_of_accounts > 0 %}
                    AND
                {% else %}
                    WHERE
                {% endif %}
                {% for filter in regexp_filters %}
                    {% unless forloop.first %}
                        AND 
                    {% endunless %}
                    REGEXP_CONTAINS({{filter.dimension}}, r"{{filter.expression}}") = {{filter.result}}
                {% endfor %}
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
