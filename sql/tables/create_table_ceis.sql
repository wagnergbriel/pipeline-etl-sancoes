CREATE TABLE IF NOT EXISTS sancoes.ceis (
    skSancao BIGSERIAL PRIMARY KEY,
    cadastro TEXT,
    cpfCnpj TEXT,
    nomeSancionado TEXT,
    ufSancionado TEXT,
    categoriaSancao TEXT,
    orgao TEXT,
    dataPublicacao DATE,
    linkDetalhamento TEXT,
    TipoDePessoa TEXT,
    cpfCnpjFormatado TEXT,
    DataDaCarga TIMESTAMP,
    ArquivoFonte TEXT
);