{% assign dimensions = vars.tiktok_ads.models.tiktok__ad_report.dimensions %}
{% assign active_dimensions = dimensions | where: 'active', true %}
{% assign dimensions = dimensions | where: 'default', true | concat: active_dimensions  %}
{% assign metrics = vars.tiktok_ads.models.tiktok__ad_report.metrics %}
{% assign active_metrics = metrics | where: 'active', true %}
{% assign metrics = metrics | where: 'default', true | concat: active_metrics  %}
{% assign active = vars.tiktok_ads.active %}
{% assign account_id = vars.tiktok_ads.account_ids %}
{% assign table_active = vars.tiktok_ads.models.tiktok__ad_report.active %}
{% assign dataset_id = vars.output_dataset_id %}
{% assign table_id = 'tiktok__ad_report_test' %}
{% assign source_dataset_id = '' %}
{% assign source_table_id = 'ads_basic_data_metrics_by_day_report' %}
{% assign number_of_accounts = vars.tiktok_ads.account_ids | size %}


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
              , max(date(stat_time_day)) as max_data_date
            from {{source_dataset_id}}.{{source_table_id}}
        )

        , ad as(
            select
                *
            from
                {{source_dataset_id}}.ad_history
        )
        
        , campaign as(
            select
                *
            from
                {{source_dataset_id}}.campaign_history
        )

        , account as(
            select
                *
            from
                {{source_dataset_id}}.ad_accounts_history
        )
        
        , metrics as(
            select
                date(stat_time_day) as date
                , ad_id
                , _gn_id
                , sum(spend) as cost
                , sum(clicks) as clicks
                , sum(real_time_conversion) as real_time_conversion
                , sum(impressions) as impressions
                , sum(conversion) as conversions
                , sum(result) as result
                , sum(real_time_result) as real_time_result
            from
                {{source_dataset_id}}.{{source_table_id}}
            group by
                1,2,3
        )

        , attribution as(
            select
                date(stat_time_day) as date
                , ad_id
                , _gn_id
                , sum(cta_registration) as cta_registration
                , sum(cta_purchase) as cta_purchase
                , sum(cta_conversion) as cta_conversion
                , sum(cta_app_install) as cta_app_install
                , sum(vta_purchase) as vta_purchase
                , sum(vta_registration) as vta_registration
                , sum(vta_app_install) as vta_app_install
                , sum(vta_conversion) as vta_conversion
            from
                {{source_dataset_id}}.ads_attribution_metrics_by_day_report
            group by
                1,2,3
        )

        , engagement as(
            select
                date(stat_time_day) as date
                , ad_id
                , _gn_id
                , sum(clicks_on_music_disc) as clicks_on_music_disc
                , sum(follows) as follows
                , sum(shares) as shares
                , sum(comments) as comments
                , sum(likes) as likes
                , sum(profile_visits) as profile_visits
            from
                {{source_dataset_id}}.ads_engagement_metrics_by_day_report
            group by
                1,2,3
        )

        , event as(
            select
                date(stat_time_day) as date
                , ad_id
                , _gn_id
                , sum(subscribe) as subscribe
                , sum(start_trial) as start_trial
                , sum(search) as search
                , sum(ratings) as ratings
                , sum(login) as login
                , sum(loan_disbursement) as loan_disbursement
                , sum(loan_credit) as loan_credit
                , sum(loan_apply) as loan_apply
                , sum(in_app_ad_impr) as in_app_ad_impr
                , sum(in_app_ad_click) as in_app_ad_click
                , sum(sales_lead) as sales_lead
                , sum(unlock_achievement) as unlock_achievement
                , sum(achieve_level) as achieve_level
                , sum(spend_credits) as spend_credits
                , sum(create_gamerole) as create_gamerole
                , sum(join_group) as join_group
                , sum(create_group) as create_group
                , sum(complete_tutorial) as complete_tutorial
                , sum(launch_app) as launch_app
                , sum(add_to_wishlist) as add_to_wishlist
                , sum(add_payment_info) as add_payment_info
                , sum(next_day_open) as next_day_open
                , sum(view_content) as view_content
                , sum(checkout) as checkout
                , sum(app_event_add_to_cart) as app_event_add_to_cart
                , sum(purchase) as purchase
                , sum(registration) as registration
                , sum(app_install) as app_install
                , sum(real_time_app_install) as real_time_app_install
            from
                {{source_dataset_id}}.ads_in_app_event_metrics_by_day_report
            group by
                1,2,3
        )

        , video as(
            select
                date(stat_time_day) as date
                , ad_id
                , _gn_id
                , sum(video_views_p100) as video_views_p100
                , sum(video_views_p75) as video_views_p75
                , sum(video_views_p50) as video_views_p50
                , sum(video_views_p25) as video_views_p25
                , sum(video_watched_6s) as video_watched_6s
                , sum(video_watched_2s) as video_watched_2s
                , sum(video_play_actions) as video_play_actions
            from
                {{source_dataset_id}}.ads_video_play_metrics_by_day_report
            group by
                1,2,3
        )

        , page as(
            select
                date(stat_time_day) as date
                , ad_id
                , _gn_id
                , sum(complete_payment * value_per_complete_payment) as revenue
                , sum(add_billing) as add_billing
                , sum(button_click) as button_click
                , sum(download_start) as download_start
                , sum(form) as form
                , sum(initiate_checkout) as initiate_checkout
                , sum(on_web_add_to_wishlist) as on_web_add_to_wishlist
                , sum(on_web_order) as on_web_order
                , sum(on_web_subscribe) as on_web_subscribe
                , sum(online_consult) as online_consult
                , sum(product_details_page_browse) as product_details_page_browse
                , sum(user_registration) as user_registration
                , sum(web_event_add_to_cart) as web_event_add_to_cart
            from
                {{source_dataset_id}}.ads_page_event_metrics_by_day_report
            group by
                1,2,3
        )
        
        , api as(
            select 
                metrics.date
                , metrics.ad_id
                , metrics._gn_id
                -- Metrics
                , metrics.cost
                , metrics.clicks
                , metrics.impressions
                , metrics.conversions
                , metrics.result
                , metrics.real_time_result
                , metrics.real_time_conversion
                -- sync info
                , sync_info.max_synced_at as last_synced_at
                , sync_info.max_data_date as last_data_date
                -- ad
                , ad.ad_id
                , ad.ad_name
                , ad.adgroup_id
                , ad.adgroup_name
                , ad.advertiser_id
                --campaign
                , campaign.campaign_id
                , campaign.campaign_name
                , campaign.objective_type
                --account
                ,account.name as advertiser_name
                ,account.currency
                -- Attribution
                , attribution.cta_app_install
                , attribution.cta_registration
                , attribution.cta_purchase
                , attribution.cta_conversion
                , attribution.vta_purchase
                , attribution.vta_registration
                , attribution.vta_app_install
                , attribution.vta_conversion
                -- Engagement
                , engagement.clicks_on_music_disc
                , engagement.follows
                , engagement.shares
                , engagement.comments
                , engagement.likes
                , engagement.profile_visits
                -- Events
                , event.subscribe
                , event.start_trial
                , event.search
                , event.ratings
                , event.login
                , event.loan_disbursement
                , event.loan_credit
                , event.loan_apply
                , event.in_app_ad_impr
                , event.in_app_ad_click
                , event.sales_lead
                , event.unlock_achievement
                , event.achieve_level
                , event.spend_credits
                , event.create_gamerole
                , event.join_group
                , event.create_group
                , event.complete_tutorial
                , event.launch_app
                , event.add_to_wishlist
                , event.add_payment_info
                , event.next_day_open
                , event.view_content
                , event.checkout
                , event.app_event_add_to_cart
                , event.purchase
                , event.registration
                , event.app_install
                , event.real_time_app_install
                -- Video
                , video.video_views_p100
                , video.video_views_p75
                , video.video_views_p50
                , video.video_views_p25
                , video.video_watched_6s
                , video.video_watched_2s
                , video.video_play_actions
                --page
                , page.revenue
                , page.add_billing
                , page.button_click
                , page.download_start
                , page.form
                , page.initiate_checkout
                , page.on_web_add_to_wishlist
                , page.on_web_order
                , page.on_web_subscribe
                , page.online_consult
                , page.product_details_page_browse
                , page.user_registration
                , page.web_event_add_to_cart
                --not available
                , '' as dpa_target_audience_type
                , '' as promotion_type
            from 
                metrics
            left join
                page on metrics.ad_id = page.ad_id and metrics.date = page.date
            left join attribution
                on metrics.ad_id = attribution.ad_id and metrics._gn_id = attribution._gn_id
            left join engagement
                on attribution.ad_id = engagement.ad_id and attribution._gn_id = engagement._gn_id
            left join event
                on engagement.ad_id = event.ad_id and engagement._gn_id = event._gn_id
            left join video
                on event.ad_id = video.ad_id and event._gn_id = video._gn_id
            left join ad
                on metrics.ad_id = ad.ad_id
            left join campaign
                on ad.campaign_id = campaign.campaign_id
            left join account
                on campaign.advertiser_id = account.advertiser_id
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
                where cast(advertiser_id as int64) in(
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
