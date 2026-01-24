-- State names data
-- Grain: 1 row per state

CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_state` AS
SELECT 'AC' AS state_code, 'Acre' AS state_full, 'BR-AC' AS state_iso UNION ALL
SELECT 'AL', 'Alagoas', 'BR-AL' UNION ALL
SELECT 'AP', 'Amapá', 'BR-AP' UNION ALL
SELECT 'AM', 'Amazonas', 'BR-AM' UNION ALL
SELECT 'BA', 'Bahia', 'BR-BA' UNION ALL
SELECT 'CE', 'Ceará', 'BR-CE' UNION ALL
SELECT 'DF', 'Distrito Federal', 'BR-DF' UNION ALL
SELECT 'ES', 'Espírito Santo', 'BR-ES' UNION ALL
SELECT 'GO', 'Goiás', 'BR-GO' UNION ALL
SELECT 'MA', 'Maranhão', 'BR-MA' UNION ALL
SELECT 'MT', 'Mato Grosso', 'BR-MT' UNION ALL
SELECT 'MS', 'Mato Grosso do Sul', 'BR-MS' UNION ALL
SELECT 'MG', 'Minas Gerais', 'BR-MG' UNION ALL
SELECT 'PA', 'Pará', 'BR-PA' UNION ALL
SELECT 'PB', 'Paraíba', 'BR-PB' UNION ALL
SELECT 'PR', 'Paraná', 'BR-PR' UNION ALL
SELECT 'PE', 'Pernambuco', 'BR-PE' UNION ALL
SELECT 'PI', 'Piauí', 'BR-PI' UNION ALL
SELECT 'RJ', 'Rio de Janeiro', 'BR-RJ' UNION ALL
SELECT 'RN', 'Rio Grande do Norte', 'BR-RN' UNION ALL
SELECT 'RS', 'Rio Grande do Sul', 'BR-RS' UNION ALL
SELECT 'RO', 'Rondônia', 'BR-RO' UNION ALL
SELECT 'RR', 'Roraima', 'BR-RR' UNION ALL
SELECT 'SC', 'Santa Catarina', 'BR-SC' UNION ALL
SELECT 'SP', 'São Paulo', 'BR-SP' UNION ALL
SELECT 'SE', 'Sergipe', 'BR-SE' UNION ALL
SELECT 'TO', 'Tocantins', 'BR-TO';
