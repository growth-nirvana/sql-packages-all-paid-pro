{% assign dimensions = vars.linkedin_ad_analytics.models.linkedin__campaign_report.dimensions %}
{% assign active_dimensions = dimensions | where: 'active', true %}
{% assign dimensions = dimensions | where: 'default', true | concat: active_dimensions  %}
{% assign metrics = vars.linkedin_ad_analytics.models.linkedin__campaign_report.metrics %}
{% assign active_metrics = metrics | where: 'active', true %}
{% assign metrics = metrics | where: 'default', true | concat: active_metrics  %}
{% assign account_id = vars.linkedin_ad_analytics.account_ids %}
{% assign active = vars.linkedin_ad_analytics.active %}
{% assign table_active = vars.linkedin_ad_analytics.models.linkedin__campaign_report.active %}
{% assign dataset_id = vars.output_dataset_id %}
{% assign table_id = vars.linkedin_ad_analytics.models.linkedin__campaign_report.table_id %}
{% assign source_dataset_id = vars.linkedin_ad_analytics.source_dataset_id %}
{% assign number_of_accounts = vars.linkedin_ad_analytics.account_ids | size %}
{% assign source_table_id = 'ad_analytics_by_campaign' %}


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
              max(datetime(_fivetran_synced, "{{ vars.timezone }}")) as max_synced_at
              , max(date(day)) as max_data_date
            from {{source_dataset_id}}.{{source_table_id}}
        )

        , campaign_groups as(
            select
                *
            from
                {{source_dataset_id}}.campaign_group_history
            qualify rank() over(partition by id order by last_modified_time desc) = 1
            and row_number() over(partition by id, last_modified_time) = 1
        )

        , campaigns as(
            select
                *
            from
                {{source_dataset_id}}.campaign_history
            qualify rank() over(partition by id order by last_modified_time desc) = 1
            and row_number() over(partition by id, last_modified_time) = 1
        )
        
        , accounts as(
            select
                *
            from
                {{source_dataset_id}}.account_history
            qualify rank() over(partition by id order by last_modified_time desc) = 1
            and row_number() over(partition by id, last_modified_time) = 1 
        )
        
        , report as(
            select
                date(day) as date
                , campaign_id
                , sum(cost_in_usd) as cost
                , sum(clicks) as clicks
                , sum(impressions) as impressions
                , sum(external_website_conversions) as conversions
                , sum(0) as revenue
                , sum(comments) as comments
                , sum(follows) as follows
                , sum(shares) as shares
                , sum(landing_page_clicks) as landing_page_clicks
            from
                {{source_dataset_id}}.{{source_table_id}}
            group by
                1,2
        )

        , api as(
            select
                report.date
                , report.campaign_id
                , campaigns.name as campaign_name
                , campaign_groups.name as campaign_group_name
                , campaign_groups.id as campaign_group_id
                , accounts.id as account_id
                , accounts.name as account_name
                , report.clicks
                , report.cost
                , report.impressions
                , report.conversions
                , report.revenue
                , report.comments
                , report.follows
                , report.shares
                , report.landing_page_clicks
                , SAFE_CAST(NULL AS TIMESTAMP) as run_schedule_start
                , SAFE_CAST(NULL AS TIMESTAMP) as campaign_group_run_schedule_start
                , sync_info.max_synced_at as last_synced_at
                , sync_info.max_data_date as last_data_date
            from
                report
            left join
                campaigns
            on
                report.campaign_id = campaigns.id
            left join
                campaign_groups
            on
                campaigns.campaign_group_id = campaign_groups.id
            left join
                accounts
            on
                campaigns.account_id = accounts.id
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

{%- assign external_project_active = vars.external_project.active -%}
{%- assign external_project_id = vars.external_project.project_id -%}
{%- assign external_project_location = vars.external_project.schema_settings.location -%}
{%- assign external_project_dataset_description = vars.external_project.schema_settings.description -%}
{%- assign external_project_dataset_friendly_name = vars.external_project.schema_settings.friendly_name -%}

{%- if external_project_active -%}
    CREATE SCHEMA IF NOT EXISTS `{{external_project_id}}`.`{{dataset_id}}`
    options(
        location='{{external_project_location}}'
        , friendly_name="{{external_project_dataset_friendly_name}}"
        , description="{{external_project_dataset_description}}"
    );

    CREATE OR REPLACE TABLE 
        `{{external_project_id}}`.`{{dataset_id}}`.`{{table_id}}` (
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
        AS (
            select * from `{{dataset_id}}`.`{{table_id}}`
        )
    {% endif %}
    ;
{%- endif -%}