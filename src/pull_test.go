package dumper

import (
	"testing"
	"time"

	"github.com/joho/godotenv"
)

func TestPullUsers(t *testing.T) {

	godotenv.Load("./test.env")
	conn := GetConn()

	type User struct {
		Id           int     `field:"CD_USUARIO"`
		PermissionId int     `field:"CD_PERMISSAO"`
		StoreId      int     `field:"CD_FILIAL"`
		Name         string  `field:"NOME"`
		Login        string  `field:"LOGIN"`
		Password     string  `field:"SENHA"`
		Enabled      string  `field:"PODEVENDER"`
		Cashier      string  `field:"CAIXA"`
		MaxDiscount  float64 `field:"TX_DESCONTO_MAXIMO"`
		Send         string  `field:"ENVIADO"`
	}

	pull := Pull[User]{
		DB:    conn,
		Query: "SELECT * FROM USUARIOS u",
	}

	data, err := pull.Run()

	if err != nil {
		t.Error("Expected err to be nil")
		t.Error(err)
	}

	if data == nil {
		t.Error("Expected data to be not nil")
	}

	if err != nil {
		t.Error("Expected err to be nil")
		t.Error(err)
	}

	if len(data) == 0 {
		t.Error("Expected data to have at least one row")
	}

	if len(data) == 62 {
		t.Error("Expected data to have 62 rows")
	}
}

func TestPullUsersSwitchKinds(t *testing.T) {

	err := godotenv.Load("./test.env")
	conn := GetConn()

	if err != nil {
		t.Error("Expected err to be nil")
		t.Error(err)
	}

	type User struct {
		Id           int    `field:"CD_USUARIO"`
		PermissionId int    `field:"CD_PERMISSAO"`
		StoreId      string `field:"CD_FILIAL"`
		Name         string `field:"NOME"`
		Login        string `field:"LOGIN"`
		Password     string `field:"SENHA"`
		Enabled      string `field:"PODEVENDER"`
		Cashier      string `field:"CAIXA"`
		MaxDiscount  string `field:"TX_DESCONTO_MAXIMO"`
		Send         string `field:"ENVIADO"`
	}

	pull := Pull[User]{
		DB:    conn,
		Query: "SELECT * FROM USUARIOS u",
	}

	data, err := pull.Run()

	if err != nil {
		t.Error("Expected err to be nil")
		t.Error(err)
	}

	if data == nil {
		t.Error("Expected data to be not nil")
	}

	if err != nil {
		t.Error("Expected err to be nil")
		t.Error(err)
	}

	if len(data) == 0 {
		t.Error("Expected data to have at least one row")
	}

	if len(data) == 62 {
		t.Error("Expected data to have 62 rows")
	}
}

func TestPullUsersMissingFields(t *testing.T) {

	err := godotenv.Load("./test.env")
	conn := GetConn()

	if err != nil {
		t.Error("Expected err to be nil")
		t.Error(err)
	}

	type User struct {
		Id           int    `field:"CD_USUARIO"`
		PermissionId int    `field:"CD_PERMISSAO"`
		StoreId      int    `field:"CD_FILIAL"`
		Name         string `field:"NOME"`
		Enabled      string `field:"PODEVENDER"`
		Cashier      string `field:"CAIXA"`
		MaxDiscount  string `field:"TX_DESCONTO_MAXIMO"`
		Send         string `field:"ENVIADO"`
	}

	pull := Pull[User]{
		DB:    conn,
		Query: "SELECT * FROM USUARIOS u",
	}

	data, err := pull.Run()

	if err != nil {
		t.Error("Expected err to be nil")
		t.Error(err)
	}

	if data == nil {
		t.Error("Expected data to be not nil")
	}

	if err != nil {
		t.Error("Expected err to be nil")
		t.Error(err)
	}

	if len(data) == 0 {
		t.Error("Expected data to have at least one row")
	}

	columns, err := pull.GetColumns()
	if err != nil {
		t.Error("Expected err during GetCollumns to be nil")
		t.Error(err)
	}

	if len(columns) == 8 {
		t.Error("Expected user to have the differente number of fields as the columns")
	}

}

func TestPullComplexQuery(t *testing.T) {

	godotenv.Load("./test.env")
	conn := GetConn()

	type Product struct {
		ProductID                     int64     `field:"ID_PRODUTO"`
		EAN                           string    `field:"CODIGO_BARRAS_1"`
		Description                   string    `field:"DESCRICAO"`
		LastUpdate                    time.Time `field:"DT_ALTERACAO"`
		Price                         float64   `field:"PRECO_VENDA_1"`
		DiscountPrice                 float64   `field:"PRECO_PROMOCAO_1"`
		Quantity                      int       `field:"QUANTIDADEINICIAL"`
		PriceQuantity                 float64   `field:"PRECO_VENDA"`
		TaxNCM                        string    `field:"NCM"`
		TaxCFOP                       string    `field:"CD_CFOP"`
		TaxCEST                       string    `field:"CEST"`
		TaxOrigem                     string    `field:"ORIGEM"`
		TaxICMSSubstituicaoTributaria string    `field:"CST_ICMS"`
		TaxICMSAliquota               float64   `field:"ALIQUOTA_ICMS"`
		TaxAliquotaPIS                float64   `field:"ALIQUOTA_PIS"`
		TaxAliquotaCOFINS             float64   `field:"ALIQUOTA_COFINS"`
		TaxCSTPis                     float64   `field:"CST_PIS_COFINS_ENTRADA"`
		TaxCSTCofins                  float64   `field:"CST_PIS_COFINS_SAIDA"`
		TaxFCP                        float64   `field:"FCP"`
		TaxCodigoBeneficio            string    `field:"CODBENEFICIO"`
		TaxMotivoDesoneracao          string    `field:"DESONERACAOMOTIVO"`
		TaxIcmsDesoneracao            float64   `field:"DESONERACAOICMS"`
		TaxFcpDesoneracao             float64   `field:"DESONERACAOFCP"`
		TaxImpNacional                float64   `field:"IMPNACIONAL"`
		TaxImpEstadual                float64   `field:"IMPESTADUAL"`
		TaxImpMunicipal               float64   `field:"IMPMUNICIPAL"`
	}

	pull := Pull[Product]{
		DB: conn,
		Query: `
		SELECT FIRST 500
			p.id_produto,
			p.codigo_barras_1,
			p.descricao,
			p.dt_alteracao,
			case when p.preco_venda_1 is null then 0 else p.preco_venda_1 end AS preco_venda_1,
			case when p.preco_promocao_1 is null then 0 else p.preco_promocao_1 end AS preco_promocao_1,
			case when pqnt.quantidadeinicial is null then 0 else pqnt.quantidadeinicial end AS quantidadeinicial,
			case when pqnt.preco_venda is null then 0 else pqnt.preco_venda end AS preco_venda,
			case when pf.ncm is null then '' else pf.ncm end AS ncm,
			case when pf.cd_cfop is null then '' else pf.cd_cfop end AS cd_cfop,
			case when pf.cest is null then '' else pf.cest end AS cest,
			case when pf.origem is null then '' else pf.origem end AS origem,
			case when pf.cst_icms is null then '' else pf.cst_icms end AS cst_icms,
			case when pf.aliquota_icms is null then 0 else pf.aliquota_icms end AS aliquota_icms,
			case when pf.aliquota_pis is null then 0 else pf.aliquota_pis end AS aliquota_pis,
			case when pf.aliquota_cofins is null then 0 else pf.aliquota_cofins end AS aliquota_cofins,
			case when pf.cst_pis_cofins_entrada is null then 0 else pf.cst_pis_cofins_entrada end AS cst_pis_cofins_entrada,
			case when pf.cst_pis_cofins_saida is null then 0 else pf.cst_pis_cofins_saida end AS cst_pis_cofins_saida,
			case when pf.fcp is null then 0 else pf.fcp end AS fcp,
			case when pf.codbeneficio is null then '' else pf.codbeneficio end AS codbeneficio,
			case when pf.desoneracaomotivo is null then '' else pf.desoneracaomotivo end AS desoneracaomotivo,
			case when pf.desoneracaoicms is null then 0 else pf.desoneracaoicms end AS desoneracaoicms,
			case when pf.desoneracaofcp is null then 0 else pf.desoneracaofcp end AS desoneracaofcp,
			0 AS ImpNacional,
			0 AS ImpEstadual,
			0 AS ImpMunicipal
		FROM
			produtos AS p
			INNER JOIN produtos_fisco AS pf ON p.id_produto = pf.id_produto
			LEFT JOIN produtos_preco_quantidade AS pqnt ON p.id_produto = pqnt.id_produto
		`,
	}

	data, err := pull.Run()

	if err != nil {
		t.Error("Expected err to be nil")
		t.Error(err)
	}

	if data == nil {
		t.Error("Expected data to be not nil")
	}

	if err != nil {
		t.Error("Expected err to be nil")
		t.Error(err)
	}

	if len(data) == 0 {
		t.Error("Expected data to have at least one row")
	}

	if len(data) != 500 {
		t.Error("Expected data to have 500 rows")
	}
}
