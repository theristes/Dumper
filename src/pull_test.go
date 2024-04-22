package dumper

import (
	"reflect"
	"testing"
	"time"
	"github.com/joho/godotenv"
)

func TestPullSimpleQueryFirebird(t *testing.T) {

	godotenv.Load("./test.env")
	conn := GetFirebirdConn()

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

}
func TestPullSimpleQueryFirebirdWithArgs(t *testing.T) {

	err := godotenv.Load("./test.env")
	conn := GetFirebirdConn()

	if err != nil {
		t.Error("Expected err to be nil")
		t.Error(err)
	}

	type User struct {
		Id           int    `field:"CD_USUARIO"`
		PermissionId int    `field:"CD_PERMISSAO"`
		StoreId      int    `field:"CD_FILIAL"`
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
		Query: "SELECT * FROM USUARIOS u WHERE u.CD_USUARIO = ?",
		Args:  []any{67},
	}

	data, err := pull.Run()

	if err != nil {
		t.Error("Expected err to be nil")
		t.Error(err)
	}

	if data == nil {
		t.Error("Expected data to be not nil")
	}

	if len(data) == 0 {
		t.Error("Expected data to have at least one row")
	}
}

func TestPullSimpleQueryFirebirdDifferentKinds(t *testing.T) {

	err := godotenv.Load("./test.env")
	conn := GetFirebirdConn()

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

func TestPullQueryFirebirdMissingFields(t *testing.T) {

	err := godotenv.Load("./test.env")
	conn := GetFirebirdConn()

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

func TestPullComplexQueryFirebird(t *testing.T) {

	godotenv.Load("./test.env")
	conn := GetFirebirdConn()

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
}

func TestPullSimpleQueryMySQL(t *testing.T) {

	err := godotenv.Load("./test.env")
	conn := GetMySQLConn()

	if err != nil {
		t.Error("Expected err to be nil")
		t.Error(err)
	}

	type Company struct {
		Id          int       `field:"id"`
		CompanyId   int       `field:"id_company"`
		OrderId     string    `field:"order_id"`
		SerieNfce   int       `field:"serie_nfce"`
		NumeroNfce  int       `field:"numero_nfce"`
		ChaveNfce   string    `field:"chave_nfce"`
		TipoEmissao string    `field:"tipo_emissao"`
		Protocolo   string    `field:"protocolo"`
		Data        time.Time `field:"data"`
		Hora        time.Time `field:"hora"`
		Status      string    `field:"status"`
	}

	pull := Pull[Company]{
		DB:    conn,
		Query: "SELECT * FROM nfce",
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
}

func TestPullSimpleQueryDifferentKindsMySQL(t *testing.T) {

	err := godotenv.Load("./test.env")
	conn := GetMySQLConn()

	if err != nil {
		t.Error("Expected err to be nil")
		t.Error(err)
	}

	type Company struct {
		Id          int    `field:"id"`
		CompanyId   int    `field:"id_company"`
		OrderId     int    `field:"order_id"`
		SerieNfce   string `field:"serie_nfce"`
		NumeroNfce  string `field:"numero_nfce"`
		// ChaveNfce   string `field:"chave_nfce"`
		// TipoEmissao string `field:"tipo_emissao"`
		// Protocolo   string `field:"protocolo"`
		// Data        string `field:"data"`
		// Hora        string `field:"hora"`
		// Status      string `field:"status"`
	}

	pull := Pull[Company]{
		DB:    conn,
		Query: "SELECT * FROM nfce",
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
}

func TestHandleFields(t *testing.T) {
	type TestStruct struct {
		IntField    int
		StringField string
		Float32Field float32
		Float64Field float64
		BoolField   bool
		SliceField  []byte
		StructField time.Time
	}

	testCases := []struct {
		Name         string
		ReflectValue reflect.Value
		FieldName    string
		DBCellValue  interface{}
		Expected     TestStruct
	}{
		{
			Name:         "Handle int field",
			ReflectValue: reflect.ValueOf(&TestStruct{}).Elem(),
			FieldName:    "IntField",
			DBCellValue:  42,
			Expected: TestStruct{
				IntField: 42,
			},
		},
		{
			Name:         "Handle string field",
			ReflectValue: reflect.ValueOf(&TestStruct{}).Elem(),
			FieldName:    "StringField",
			DBCellValue:  "hello",
			Expected: TestStruct{
				StringField: "hello",
			},
		},
		{
			Name:         "Handle float32 field",
			ReflectValue: reflect.ValueOf(&TestStruct{}).Elem(),
			FieldName:    "Float32Field",
			DBCellValue:  3.14,
			Expected: TestStruct{
				Float32Field: 3.14,
			},
		},
		{
			Name:         "Handle float64 field",
			ReflectValue: reflect.ValueOf(&TestStruct{}).Elem(),
			FieldName:    "Float64Field",
			DBCellValue:  3.14159,
			Expected: TestStruct{
				Float64Field: 3.14159,
			},
		},
		{
			Name:         "Handle bool field",
			ReflectValue: reflect.ValueOf(&TestStruct{}).Elem(),
			FieldName:    "BoolField",
			DBCellValue:  true,
			Expected: TestStruct{
				BoolField: true,
			},
		},
		{
			Name:         "Handle slice field",
			ReflectValue: reflect.ValueOf(&TestStruct{}).Elem(),
			FieldName:    "SliceField",
			DBCellValue:  []byte{1, 2, 3},
			Expected: TestStruct{
				SliceField: []byte{1, 2, 3},
			},
		},
		{
			Name:         "Handle struct field",
			ReflectValue: reflect.ValueOf(&TestStruct{}).Elem(),
			FieldName:    "StructField",
			DBCellValue:  time.Date(2022, time.January, 1, 0, 0, 0, 0, time.UTC),
			Expected: TestStruct{
				StructField: time.Date(2022, time.January, 1, 0, 0, 0, 0, time.UTC),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			handleFields(tc.ReflectValue, tc.FieldName, tc.DBCellValue)

			actual := tc.ReflectValue.Interface().(TestStruct)
			if !reflect.DeepEqual(actual, tc.Expected) {
				t.Errorf("Expected %+v, but got %+v", tc.Expected, actual)
			}
		})
	}
}

func TestGetFieldNameByColumnName(t *testing.T) {
	type TestStruct struct {
		IntField    int    `field:"column1"`
		StringField string `field:"column2"`
		FloatField  float64
	}

	testCases := []struct {
		Name         string
		ReflectValue reflect.Value
		ColumnName   string
		Expected     string
	}{
		{
			Name:         "Matching column name",
			ReflectValue: reflect.ValueOf(TestStruct{}),
			ColumnName:   "column1",
			Expected:     "IntField",
		},
		{
			Name:         "Non-matching column name",
			ReflectValue: reflect.ValueOf(TestStruct{}),
			ColumnName:   "column3",
			Expected:     "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			actual := getFieldNameByColumnName(tc.ReflectValue, tc.ColumnName)
			if actual != tc.Expected {
				t.Errorf("Expected field name %s, but got %s", tc.Expected, actual)
			}
		})
	}
}