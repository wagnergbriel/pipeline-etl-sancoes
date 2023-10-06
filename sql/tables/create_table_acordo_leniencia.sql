CREATE TABLE IF NOT EXISTS sancoes.acordo_leniencia (
    skAcordoLeniencia INT PRIMARY KEY,
    dataInicioAcordo DATE,
    dataFimAcordo DATE,
    orgaoResponsavel TEXT,
    situacaoAcordo TEXT,
    nomeInformadoOrgaoResponsavel TEXT,
    razaoSocial TEXT,
    nomeFantasia TEXT,
    cnpj TEXT,
    cnpjFormatado TEXT,
    DataDaCarga TIMESTAMP,
    ArquivoFonte TEXT
);
