package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/caarlos0/env/v6"
	"github.com/jackc/pgx/v5"
)

/*********************
 *
 * some things about the data:
 *
 * complete list of countries:
 Australia
 Brazil
 Canada
 China
 France
 Germany
 India
 Japan
 Russia
 USA
 * complete list of EnergyTypes:
 Geothermal
 Biomass
 Wind
 Solar
 Hydro
 * complete list of years:
years are 200-2023 inclusive
*/
 
type Config struct {
	User     string `env:"DB_USER,required"`
	Password string `env:"DB_PASSWORD,required"`
}

type BigDataPoint struct {
	Country                             string
	Year                                int
	EnergyType                          string
	ProductionGWh                       float32
	InstalledCapacityMW                 float32
	InvestmentsUSD                      float32
	Population                          int
	GDP                                 float32
	EnergyConsumption                   float32
	EnergyExports                       float32
	EnergyImports                       float32
	CO2Emissions                        float32
	RenewableEnergyJobs                 int
	GovernmentPolicies                  int
	RnDExpenditure                      float32
	RenewableEnergyTargets              int
	AverageAnnualTemperature            float32
	AnnualRainfall                      float32
	SolarIrradiance                     float32
	WindSpeed                           float32
	HydroPotential                      float32
	GeothermalPotential                 float32
	BiomassAvailability                 float32
	EnergyStorageCapacity               float32
	GridIntegrationCapability           float32
	ElectricityPrices                   float32
	EnergySubsidies                     float32
	InternationalAidForRenewables       float32
	PublicAwareness                     float32
	EnergyEfficiencyPrograms            int
	UrbanizationRate                    float32
	IndustrializationRate               float32
	EnergyMarketLiberalization          int
	RenewableEnergyPatents              int
	EducationalLevel                    float32
	TechnologyTransferAgreements        int
	RenewableEnergyEducationPrograms    int
	LocalManufacturingCapacity          float32
	ImportTariffsOnEnergyEquipment      float32
	ExportIncentivesForEnergyEquipment  float32
	NaturalDisasters                    int
	PoliticalStability                  float32
	CorruptionPerceptionIndex           float32
	RegulatoryQuality                   float32
	RuleOfLaw                           float32
	ControlOfCorruption                 float32
	EconomicFreedomIndex                float32
	EaseOFDoingBusiness                 float32
	InnovationIndex                     float32
	NumberOfResearchInstitutions        int
	NumberOFRenewableEnergyConferences  int
	NumberOfRenewableEnergyPublications int
	EnergySectorWorkforce               int
	ProportionOfEnergyFromRenewables    float32
	PublicPrivatePartnershipsInEnergy   int
	RegionalRenewableEnergyCooperation  int
}

func connect() (*pgx.Conn, error) {
	cfg := Config{}
	err := env.Parse(&cfg)
	if err != nil {
		panic(err)
	}
	sqlURL := "postgres://" +
		cfg.User +
		":" +
		cfg.Password +
		"@172.18.0.3:5432/go" // url provided by docker network
	conn, err := pgx.Connect(context.Background(), sqlURL)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func bigQuery(conn *pgx.Conn, dataPoints *[]BigDataPoint) {
	rows, err := conn.Query(context.Background(),
		`SELECT 
		Country, 
		Year, 
		EnergyType, 
		ProductionGWh, 
		InstalledCapacityMW, 
		InvestmentsUSD, 
		Population, 
		GDP, 
		EnergyConsumption, 
		EnergyExports, 
		EnergyImports, 
		CO2Emissions, 
		RenewableEnergyJobs, 
		GovernmentPolicies, 
		RnDExpenditure, 
		RenewableEnergyTargets, 
		AverageAnnualTemperature, 
		AnnualRainfall, 
		SolarIrradiance, 
		WindSpeed, 
		HydroPotential, 
		GeothermalPotential, 
		BiomassAvailability, 
		EnergyStorageCapacity, 
		GridIntegrationCapability, 
		ElectricityPrices, 
		EnergySubsidies, 
		InternationalAidForRenewables, 
		PublicAwareness, 
		EnergyEfficiencyPrograms, 
		UrbanizationRate, 
		IndustrializationRate, 
		EnergyMarketLiberalization, 
		RenewableEnergyPatents, 
		EducationalLevel, 
		TechnologyTransferAgreements, 
		RenewableEnergyEducationPrograms, 
		LocalManufacturingCapacity, 
		ImportTariffsOnEnergyEquipment, 
		ExportIncentivesForEnergyEquipment, 
		NaturalDisasters, 
		PoliticalStability, 
		CorruptionPerceptionIndex, 
		RegulatoryQuality, 
		RuleOfLaw, 
		ControlOfCorruption, 
		EconomicFreedomIndex, 
		EaseOFDoingBusiness, 
		InnovationIndex, 
		NumberOfResearchInstitutions, 
		NumberOFRenewableEnergyConferences, 
		NumberOfRenewableEnergyPublications, 
		EnergySectorWorkforce, 
		ProportionOfEnergyFromRenewables, 
		PublicPrivatePartnershipsInEnergy, 
		RegionalRenewableEnergyCooperation 
		from energy`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var dataPoint BigDataPoint

		err := rows.Scan(
			&dataPoint.Country,
			&dataPoint.Year,
			&dataPoint.EnergyType,
			&dataPoint.ProductionGWh,
			&dataPoint.InstalledCapacityMW,
			&dataPoint.InvestmentsUSD,
			&dataPoint.Population,
			&dataPoint.GDP,
			&dataPoint.EnergyConsumption,
			&dataPoint.EnergyExports,
			&dataPoint.EnergyImports,
			&dataPoint.CO2Emissions,
			&dataPoint.RenewableEnergyJobs,
			&dataPoint.GovernmentPolicies,
			&dataPoint.RnDExpenditure,
			&dataPoint.RenewableEnergyTargets,
			&dataPoint.AverageAnnualTemperature,
			&dataPoint.AnnualRainfall,
			&dataPoint.SolarIrradiance,
			&dataPoint.WindSpeed,
			&dataPoint.HydroPotential,
			&dataPoint.GeothermalPotential,
			&dataPoint.BiomassAvailability,
			&dataPoint.EnergyStorageCapacity,
			&dataPoint.GridIntegrationCapability,
			&dataPoint.ElectricityPrices,
			&dataPoint.EnergySubsidies,
			&dataPoint.InternationalAidForRenewables,
			&dataPoint.PublicAwareness,
			&dataPoint.EnergyEfficiencyPrograms,
			&dataPoint.UrbanizationRate,
			&dataPoint.IndustrializationRate,
			&dataPoint.EnergyMarketLiberalization,
			&dataPoint.RenewableEnergyPatents,
			&dataPoint.EducationalLevel,
			&dataPoint.TechnologyTransferAgreements,
			&dataPoint.RenewableEnergyEducationPrograms,
			&dataPoint.LocalManufacturingCapacity,
			&dataPoint.ImportTariffsOnEnergyEquipment,
			&dataPoint.ExportIncentivesForEnergyEquipment,
			&dataPoint.NaturalDisasters,
			&dataPoint.PoliticalStability,
			&dataPoint.CorruptionPerceptionIndex,
			&dataPoint.RegulatoryQuality,
			&dataPoint.RuleOfLaw,
			&dataPoint.ControlOfCorruption,
			&dataPoint.EconomicFreedomIndex,
			&dataPoint.EaseOFDoingBusiness,
			&dataPoint.InnovationIndex,
			&dataPoint.NumberOfResearchInstitutions,
			&dataPoint.NumberOFRenewableEnergyConferences,
			&dataPoint.NumberOfRenewableEnergyPublications,
			&dataPoint.EnergySectorWorkforce,
			&dataPoint.ProportionOfEnergyFromRenewables,
			&dataPoint.PublicPrivatePartnershipsInEnergy,
			&dataPoint.RegionalRenewableEnergyCooperation,
		)
		if err != nil {
			panic(err)
		}
		*dataPoints = append(*dataPoints, dataPoint)
	}
}

func runBigQuery(w http.ResponseWriter, r *http.Request) {
	var dataPoints []BigDataPoint

	conn, err := connect()
	if err != nil {
		panic(err)
	}
	bigQuery(conn, &dataPoints) //slices pass by ref but they dont modify the original unless you sent a pointer or return and assign
	fmt.Println(len(dataPoints))
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(dataPoints)
}

func main() {
	http.HandleFunc("/bigquery", runBigQuery)
	err := http.ListenAndServe(":8080", nil)
	if errors.Is(err, http.ErrServerClosed) {
		fmt.Println("server closed")
	} else if err != nil {
		fmt.Printf("error starting server: %s\n", err)
		os.Exit(1)
	}
}
