SELECT
    id_paciente,
    nome,
    data_nascimento,
    identidade_genero,
    bairro,
    raca_cor,
    ocupacao,
    religiao
FROM {{ ref('stg_dados_ficha_a') }}