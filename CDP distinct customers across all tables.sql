--the following script will take 1000 customers and show each given customers entries in each table in CDP. The purpose is to allow us to better select customers to clone from CDP

WITH recent_customers AS (
    SELECT *
   FROM `dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.customer_view` 
  --  where customer_id = '94ab21c4-34a8-4c6b-9b60-0c784760d54e'
   ORDER BY last_modified_timestamp DESC
  LIMIT 1000
)

SELECT 
    cv.customer_ID,
    COUNT( DISTINCT iv.customer_ID) AS identifiers_view,
    COUNT( DISTINCT  cdv.customer_ID) AS company_details_view,
    COUNT(  DISTINCT cav.customer_ID) AS country_association_view,
    COUNT( DISTINCT av.customer_ID) AS address_view,
    COUNT( DISTINCT  kdv.customer_ID) AS kyb_detail_view,
    COUNT( DISTINCT  btdv.customer_ID) AS business_trading_details_view,
    COUNT(  DISTINCT kybv.customer_ID) AS kyb_view,
    COUNT(  DISTINCT acav.customer_ID) AS additional_customer_attribute_view,
    COUNT(  DISTINCT csv.customer_ID) AS customer_status_view,
    COUNT(  DISTINCT cov.customer_ID) AS customer_view,
    COUNT(  DISTINCT couv.customer_ID) AS customer_organisation_unit_view,
    COUNT(  DISTINCT kycv.customer_ID) AS kyc_view,
    COUNT(  DISTINCT clv.customer_ID) AS classification_view,
    COUNT(  DISTINCT cdv2.customer_ID) AS contact_details_view,
    COUNT(  DISTINCT pbv.customer_ID) AS portfolio_brand_view,
    COUNT(  DISTINCT siv.customer_ID) AS source_identifier_view,
    COUNT(  DISTINCT cdv3.customer_ID) AS customer_detail_view,
    COUNT(  DISTINCT crhv.customer_ID) AS customer_relationship_hierarchy_view,
    COUNT( DISTINCT iv.customer_ID) + COUNT(DISTINCT cdv.customer_ID) + COUNT(DISTINCT cav.customer_ID) + COUNT(DISTINCT av.customer_ID) +
    COUNT( DISTINCT kdv.customer_ID) + COUNT(DISTINCT btdv.customer_ID) + COUNT(DISTINCT kybv.customer_ID) + COUNT(DISTINCT acav.customer_ID) +
    COUNT( DISTINCT csv.customer_ID) + COUNT(DISTINCT cov.customer_ID) + COUNT(DISTINCT couv.customer_ID) + COUNT(DISTINCT kycv.customer_ID) +
    COUNT( DISTINCT clv.customer_ID) + COUNT(DISTINCT cdv2.customer_ID) + COUNT(DISTINCT pbv.customer_ID) + COUNT(DISTINCT siv.customer_ID) +
    COUNT( DISTINCT cdv3.customer_ID) + COUNT(DISTINCT crhv.customer_ID) AS total_count
FROM 
    recent_customers cv
LEFT JOIN dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.identifiers_view iv ON cv.customer_ID = iv.customer_ID
LEFT JOIN dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.company_details_view cdv ON cv.customer_ID = cdv.customer_ID
LEFT JOIN dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.country_association_view cav ON cv.customer_ID = cav.customer_ID
LEFT JOIN dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.address_view av ON cv.customer_ID = av.customer_ID
LEFT JOIN dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.kyb_detail_view kdv ON cv.customer_ID = kdv.customer_ID
LEFT JOIN dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.business_trading_details_view btdv ON cv.customer_ID = btdv.customer_ID
LEFT JOIN dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.kyb_view kybv ON cv.customer_ID = kybv.customer_ID
LEFT JOIN dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.additional_customer_attribute_view acav ON cv.customer_ID = acav.customer_ID
LEFT JOIN dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.customer_status_view csv ON cv.customer_ID = csv.customer_ID
LEFT JOIN dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.customer_view cov ON cv.customer_ID = cov.customer_ID
LEFT JOIN dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.customer_organisation_unit_view couv ON cv.customer_ID = couv.customer_ID
LEFT JOIN dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.kyc_view kycv ON cv.customer_ID = kycv.customer_ID
LEFT JOIN dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.classification_view clv ON cv.customer_ID = clv.customer_ID
LEFT JOIN dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.contact_details_view cdv2 ON cv.customer_ID = cdv2.customer_ID
LEFT JOIN dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.portfolio_brand_view pbv ON cv.customer_ID = pbv.customer_ID
LEFT JOIN dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.source_identifier_view siv ON cv.customer_ID = siv.customer_ID
LEFT JOIN dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.customer_detail_view cdv3 ON cv.customer_ID = cdv3.customer_ID
LEFT JOIN dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view.customer_relationship_hierarchy_view crhv ON cv.customer_ID = crhv.customer_ID
GROUP BY 
    cv.customer_ID
ORDER BY total_count DESC
    ;
