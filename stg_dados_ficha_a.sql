WITH raw_data AS (
    SELECT
        *,
        -- Padronizar booleanos
        CASE
            WHEN obito IN ('0', 'false') THEN FALSE
            WHEN obito IN ('1', 'true') THEN TRUE
            ELSE NULL
        END AS obito_padronizado,
        -- Preencher valores ausentes
        COALESCE(data_cadastro, '1900-01-01') AS data_cadastro_tratada,
        COALESCE(identidade_genero, 'não informado') AS identidade_genero_tratada,
        COALESCE(data_atualizacao_cadastro, '1900-01-01') AS data_atualizacao_tratada
    FROM {{ source('raw', 'dados_ficha_a') }}
)
SELECT
    * EXCEPT (obito, data_cadastro, identidade_genero, data_atualizacao_cadastro),
    obito_padronizado AS obito,
    data_cadastro_tratada AS data_cadastro,
    identidade_genero_tratada AS identidade_genero,
    data_atualizacao_tratada AS data_atualizacao_cadastro
FROM raw_data