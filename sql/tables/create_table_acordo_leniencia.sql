CREATE TABLE IF NOT EXISTS sancoes.acordo_leniencia (
    skAcordoLeniencia INT,
    dataInicioAcordo DATE,
    dataFimAcordo DATE,
    orgaoResponsavel TEXT,
    situacaoAcordo TEXT,
    nomeInformadoOrgaoResponsavel TEXT,
    razaoSocial TEXT,
    nomeFantasia TEXT,
    cnpj VARCHAR(20),
    cnpjFormatado VARCHAR(14),
    DataDaCarga TIMESTAMP,
    ArquivoFonte VARCHAR(30)
);
