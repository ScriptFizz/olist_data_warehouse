CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.state_names_map` AS
SELECT 'AC' AS uf, 'Acre' AS state_full UNION ALL
SELECT 'AL', 'Alagoas' UNION ALL
SELECT 'AP', 'Amapá' UNION ALL
SELECT 'AM', 'Amazonas' UNION ALL
SELECT 'BA', 'Bahia' UNION ALL
SELECT 'CE', 'Ceará' UNION ALL
SELECT 'DF', 'Distrito Federal' UNION ALL
SELECT 'ES', 'Espírito Santo' UNION ALL
SELECT 'GO', 'Goiás' UNION ALL
SELECT 'MA', 'Maranhão' UNION ALL
SELECT 'MT', 'Mato Grosso' UNION ALL
SELECT 'MS', 'Mato Grosso do Sul' UNION ALL
SELECT 'MG', 'Minas Gerais' UNION ALL
SELECT 'PA', 'Pará' UNION ALL
SELECT 'PB', 'Paraíba' UNION ALL
SELECT 'PR', 'Paraná' UNION ALL
SELECT 'PE', 'Pernambuco' UNION ALL
SELECT 'PI', 'Piauí' UNION ALL
SELECT 'RJ', 'Rio de Janeiro' UNION ALL
SELECT 'RN', 'Rio Grande do Norte' UNION ALL
SELECT 'RS', 'Rio Grande do Sul' UNION ALL
SELECT 'RO', 'Rondônia' UNION ALL
SELECT 'RR', 'Roraima' UNION ALL
SELECT 'SC', 'Santa Catarina' UNION ALL
SELECT 'SP', 'São Paulo' UNION ALL
SELECT 'SE', 'Sergipe' UNION ALL
SELECT 'TO', 'Tocantins';
