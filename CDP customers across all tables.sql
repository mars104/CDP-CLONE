--the followign script shows the number of entries per customer in each table in CDP, the purpose is to show potential duplicate entries in certain tables (DQ issues)

WITH recent_customers AS (
    SELECT *
   FROM `dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.customer_view` 
    where customer_id = '94ab21c4-34a8-4c6b-9b60-0c784760d54e'
   ORDER BY last_modified_timestamp DESC
  LIMIT 1000
),
iv_counts AS (SELECT customer_ID, COUNT(*) AS identifiers_view FROM dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.identifiers_view GROUP BY customer_ID
),
cdv_counts AS ( SELECT customer_ID, COUNT(*) AS company_details_view FROM dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.company_details_view GROUP BY customer_ID
),
cav_counts AS (SELECT customer_ID, COUNT(*) AS country_association_view FROM dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.country_association_view GROUP BY customer_ID
),
av_counts AS (SELECT customer_ID, COUNT(*) AS address_view FROM dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.address_view GROUP BY customer_ID
),
kdv_counts AS (SELECT customer_ID, COUNT(*) AS kyb_detail_view FROM dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.kyb_detail_view GROUP BY customer_ID
),
btdv_counts AS (SELECT customer_ID, COUNT(*) AS business_trading_details_view FROM dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.business_trading_details_view GROUP BY customer_ID
),
kybv_counts AS (SELECT customer_ID, COUNT(*) AS kyb_view FROM dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.kyb_view GROUP BY customer_ID
),
acav_counts AS (SELECT customer_ID, COUNT(*) AS additional_customer_attribute_view FROM dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.additional_customer_attribute_view GROUP BY customer_ID
),
csv_counts AS (SELECT customer_ID, COUNT(*) AS customer_status_view FROM dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.customer_status_view GROUP BY customer_ID
),
cov_counts AS (SELECT customer_ID, COUNT(*) AS customer_view FROM dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.customer_view GROUP BY customer_ID
),
couv_counts AS (SELECT customer_ID, COUNT(*) AS customer_organisation_unit_view FROM dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.customer_organisation_unit_view GROUP BY customer_ID
),
kycv_counts AS (SELECT customer_ID, COUNT(*) AS kyc_view FROM dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.kyc_view GROUP BY customer_ID
),
clv_counts AS (SELECT customer_ID, COUNT(*) AS classification_view FROM dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.classification_view GROUP BY customer_ID
),
cdv2_counts AS (SELECT customer_ID, COUNT(*) AS contact_details_view FROM dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.contact_details_view GROUP BY customer_ID
),
pbv_counts AS ( SELECT customer_ID, COUNT(*) AS portfolio_brand_view FROM dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.portfolio_brand_view GROUP BY customer_ID
),
siv_counts AS (SELECT customer_ID, COUNT(*) AS source_identifier_view FROM dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.source_identifier_view GROUP BY customer_ID
),
cdv3_counts AS (
    SELECT customer_ID, COUNT(*) AS customer_detail_view FROM dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.customer_detail_view GROUP BY customer_ID
),crhv_counts AS (
    SELECT customer_ID, COUNT(*) AS customer_relationship_hierarchy_view FROM dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.customer_relationship_hierarchy_view GROUP BY customer_ID
)
SELECT distinct
    rc.customer_ID,
    COALESCE(iv_counts.identifiers_view, 0) AS identifiers_view,
    COALESCE(cdv_counts.company_details_view, 0) AS company_details_view,
    COALESCE(cav_counts.country_association_view, 0) AS country_association_view,
    COALESCE(av_counts.address_view, 0) AS address_view,
    COALESCE(kdv_counts.kyb_detail_view, 0) AS kyb_detail_view,
    COALESCE(btdv_counts.business_trading_details_view, 0) AS business_trading_details_view,
    COALESCE(kybv_counts.kyb_view, 0) AS kyb_view,
    COALESCE(acav_counts.additional_customer_attribute_view, 0) AS additional_customer_attribute_view,
    COALESCE(csv_counts.customer_status_view, 0) AS customer_status_view,
    COALESCE(cov_counts.customer_view, 0) AS customer_view,
    COALESCE(couv_counts.customer_organisation_unit_view, 0) AS customer_organisation_unit_view,
    COALESCE(kycv_counts.kyc_view, 0) AS kyc_view,
    COALESCE(clv_counts.classification_view, 0) AS classification_view,
    COALESCE(cdv2_counts.contact_details_view, 0) AS contact_details_view,
    COALESCE(pbv_counts.portfolio_brand_view, 0) AS portfolio_brand_view,
    COALESCE(siv_counts.source_identifier_view, 0) AS source_identifier_view,
    COALESCE(cdv3_counts.customer_detail_view, 0) AS customer_detail_view,
    COALESCE(crhv_counts.customer_relationship_hierarchy_view, 0) AS customer_relationship_hierarchy_view,
    COALESCE(iv_counts.identifiers_view, 0) +
    COALESCE(cdv_counts.company_details_view, 0) +
    COALESCE(cav_counts.country_association_view, 0) +
    COALESCE(av_counts.address_view, 0) +
    COALESCE(kdv_counts.kyb_detail_view, 0) +
    COALESCE(btdv_counts.business_trading_details_view, 0) +
    COALESCE(kybv_counts.kyb_view, 0) +
    COALESCE(acav_counts.additional_customer_attribute_view, 0) +
    COALESCE(csv_counts.customer_status_view, 0) +
    COALESCE(cov_counts.customer_view, 0) +
    COALESCE(couv_counts.customer_organisation_unit_view, 0) +
    COALESCE(kycv_counts.kyc_view, 0) +
    COALESCE(clv_counts.classification_view, 0) +
    COALESCE(cdv2_counts.contact_details_view, 0) +
    COALESCE(pbv_counts.portfolio_brand_view, 0) +
    COALESCE(siv_counts.source_identifier_view, 0) +
    COALESCE(cdv3_counts.customer_detail_view, 0) +
    COALESCE(crhv_counts.customer_relationship_hierarchy_view, 0) AS total_count
FROM 
    recent_customers rc
LEFT JOIN iv_counts ON rc.customer_ID = iv_counts.customer_ID
LEFT JOIN cdv_counts ON rc.customer_ID = cdv_counts.customer_ID
LEFT JOIN cav_counts ON rc.customer_ID = cav_counts.customer_ID
LEFT JOIN av_counts ON rc.customer_ID = av_counts.customer_ID
LEFT JOIN kdv_counts ON rc.customer_ID = kdv_counts.customer_ID
LEFT JOIN btdv_counts ON rc.customer_ID = btdv_counts.customer_ID
LEFT JOIN kybv_counts ON rc.customer_ID = kybv_counts.customer_ID
LEFT JOIN acav_counts ON rc.customer_ID = acav_counts.customer_ID
LEFT JOIN csv_counts ON rc.customer_ID = csv_counts.customer_ID
LEFT JOIN cov_counts ON rc.customer_ID = cov_counts.customer_ID
LEFT JOIN couv_counts ON rc.customer_ID = couv_counts.customer_ID
LEFT JOIN kycv_counts ON rc.customer_ID = kycv_counts.customer_ID
LEFT JOIN clv_counts ON rc.customer_ID = clv_counts.customer_ID
LEFT JOIN cdv2_counts ON rc.customer_ID = cdv2_counts.customer_ID
LEFT JOIN pbv_counts ON rc.customer_ID = pbv_counts.customer_ID
LEFT JOIN siv_counts ON rc.customer_ID = siv_counts.customer_ID
LEFT JOIN cdv3_counts ON rc.customer_ID = cdv3_counts.customer_ID
LEFT JOIN crhv_counts ON rc.customer_ID = crhv_counts.customer_ID
ORDER BY total_count DESC
