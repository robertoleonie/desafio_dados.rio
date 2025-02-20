WITH raw_data AS (
    SELECT * FROM {{ ref('dados_ficha_a_desafio') }} -- Nome da tabela original
),

boolean_transform AS (
    SELECT
        id_paciente,
        CAST(CASE WHEN obito IN (0, '0', FALSE, 'FALSE') THEN FALSE ELSE TRUE END AS BOOLEAN) AS obito,
        CAST(CASE WHEN luz_eletrica IN (0, '0', FALSE, 'FALSE') THEN FALSE ELSE TRUE END AS BOOLEAN) AS luz_eletrica,
        CAST(CASE WHEN em_situacao_de_rua IN (0, '0', FALSE, 'FALSE') THEN FALSE ELSE TRUE END AS BOOLEAN) AS em_situacao_de_rua,
        CAST(CASE WHEN frequenta_escola IN (0, '0', FALSE, 'FALSE') THEN FALSE ELSE TRUE END AS BOOLEAN) AS frequenta_escola,
        -- Continue com outras colunas booleanas...
        *
    FROM raw_data
)

SELECT * FROM boolean_transform;
