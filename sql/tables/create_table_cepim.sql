CREATE TABLE IF NOT EXISTS sancoes.cepim (
    skSancao INT,
    cadastro VARCHAR(10),
    cpfCnpj VARCHAR(20),
    nomeSancionado TEXT,
    ufSancionado VARCHAR(2),
    categoriaSancao TEXT,
    orgao TEXT,
    dataPublicacao VARCHAR(20),
    linkDetalhamento TEXT,
    TipoDePessoa VARCHAR(1),
    cpfCnpjFormatado VARCHAR(14),
    DataDaCarga TIMESTAMP,
    ArquivoFonte VARCHAR(30)
);