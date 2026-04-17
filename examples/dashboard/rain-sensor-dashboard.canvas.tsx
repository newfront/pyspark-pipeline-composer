import {
  BarChart,
  Button,
  Card,
  CardBody,
  CardHeader,
  Divider,
  Grid,
  H1,
  H2,
  H3,
  LineChart,
  PieChart,
  Pill,
  Row,
  Spacer,
  Stack,
  Stat,
  Table,
  Text,
  useCanvasState,
  useHostTheme,
} from "cursor/canvas";

const locations = [
  { name: "Mt. Waialeale, Kauai, HI, USA", precip: 176377.0, temp: 14.6, humidity: 97.7, wind: 31.9, maxWind: 64.8, rainPct: 81.7 },
  { name: "Mawsynram, Meghalaya, India", precip: 151109.0, temp: 14.3, humidity: 95.2, wind: 25.4, maxWind: 54.9, rainPct: 74.9 },
  { name: "Cherrapunji, Meghalaya, India", precip: 123475.4, temp: 15.3, humidity: 94.1, wind: 22.0, maxWind: 49.9, rainPct: 72.1 },
  { name: "Quibdo, Choco, Colombia", precip: 101076.8, temp: 25.9, humidity: 95.4, wind: 13.6, maxWind: 35.0, rainPct: 69.9 },
  { name: "Mumbai, Maharashtra, India", precip: 95993.9, temp: 27.7, humidity: 81.7, wind: 26.3, maxWind: 59.8, rainPct: 45.2 },
  { name: "Hilo, HI, USA", precip: 67460.5, temp: 22.2, humidity: 88.5, wind: 18.6, maxWind: 44.7, rainPct: 60.2 },
  { name: "Singapore", precip: 63684.3, temp: 27.5, humidity: 87.6, wind: 15.4, maxWind: 39.8, rainPct: 50.2 },
  { name: "Manaus, Amazonas, Brazil", precip: 62265.7, temp: 27.8, humidity: 90.8, wind: 11.5, maxWind: 32.8, rainPct: 54.9 },
  { name: "Milford Sound, Fiordland, NZ", precip: 61137.6, temp: 8.2, humidity: 89.4, wind: 23.5, maxWind: 54.6, rainPct: 61.8 },
  { name: "Cameron Highlands, Malaysia", precip: 49796.8, temp: 18.5, humidity: 90.7, wind: 12.1, maxWind: 34.9, rainPct: 50.1 },
  { name: "Bergen, Vestland, Norway", precip: 39724.4, temp: 6.3, humidity: 85.1, wind: 28.7, maxWind: 65.0, rainPct: 54.7 },
  { name: "Tokyo, Japan", precip: 29971.7, temp: 16.9, humidity: 69.9, wind: 16.9, maxWind: 44.0, rainPct: 35.2 },
  { name: "Juneau, AK, USA", precip: 28996.5, temp: 1.5, humidity: 81.5, wind: 24.5, maxWind: 59.8, rainPct: 51.6 },
  { name: "Reykjavik, Iceland", precip: 20596.3, temp: 3.1, humidity: 83.2, wind: 36.8, maxWind: 78.8, rainPct: 48.1 },
  { name: "London, England, UK", precip: 18918.2, temp: 12.4, humidity: 76.3, wind: 19.4, maxWind: 49.3, rainPct: 38.1 },
  { name: "Seattle, WA, USA", precip: 18051.6, temp: 10.7, humidity: 77.7, wind: 16.2, maxWind: 44.2, rainPct: 42.4 },
  { name: "San Francisco, CA, USA", precip: 7884.6, temp: 14.4, humidity: 71.2, wind: 22.5, maxWind: 52.6, rainPct: 22.0 },
  { name: "Dubai, UAE", precip: 712.6, temp: 31.3, humidity: 40.5, wind: 21.6, maxWind: 40.0, rainPct: 5.0 },
  { name: "Death Valley, CA, USA", precip: 218.9, temp: 29.9, humidity: 15.3, wind: 21.0, maxWind: 40.0, rainPct: 3.0 },
  { name: "San Pedro de Atacama, Chile", precip: 42.4, temp: 13.0, humidity: 11.6, wind: 19.0, maxWind: 35.0, rainPct: 0.9 },
];

const months = ["Jan 24","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec","Jan 25","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];

const monthlyPrecip: Record<string, number[]> = {
  "Mt. Waialeale, Kauai, HI, USA": [7549.8,6773.8,7593.2,7094.5,7558.4,7224.4,7394.6,7457.6,7428.2,7434.8,7422.4,7383.3,7350.3,6862.6,7770.1,7039.3,7736.2,7162.8,7471.3,7470.6,7327.8,7625.0,7294.0,6952.3],
  "Mawsynram, Meghalaya, India": [6404.7,5995.0,6373.4,6363.3,6446.0,6067.5,6153.8,6445.0,6306.4,6421.7,6196.9,6399.5,6637.6,5550.5,6390.1,6078.8,6311.2,6155.8,6430.6,6215.2,6284.5,6792.1,6456.3,6232.8],
  "Cherrapunji, Meghalaya, India": [5086.5,4798.5,5465.4,5217.9,5302.6,5188.1,5234.1,5345.2,5140.0,5277.4,4913.4,5210.8,5218.5,4749.4,5110.9,5244.3,5310.0,5018.6,5254.5,5125.3,5202.4,5270.2,4911.5,4879.8],
  "Bergen, Vestland, Norway": [1714.6,1613.9,1623.8,1629.8,1703.1,1633.6,1748.2,1670.0,1534.7,1705.8,1633.6,1728.2,1671.4,1521.9,1693.3,1645.6,1665.7,1577.6,1721.6,1711.7,1656.3,1681.6,1636.5,1601.8],
  "Singapore": [2755.3,2461.4,2705.3,2559.2,2705.7,2637.2,2754.8,2658.2,2610.5,2772.2,2579.0,2636.0,2726.1,2472.9,2737.1,2614.7,2680.0,2694.3,2749.9,2682.6,2516.1,2771.5,2668.5,2535.7],
  "Seattle, WA, USA": [789.6,734.6,752.5,806.0,767.4,761.1,801.6,752.5,752.7,764.9,729.1,788.7,728.8,730.0,734.0,715.5,775.1,718.2,729.3,759.7,753.7,754.3,727.8,724.4],
  "Tokyo, Japan": [1230.1,1241.6,1281.8,1359.4,1298.6,1257.9,1221.4,1322.5,1219.6,1285.5,1227.0,1266.0,1265.8,1123.3,1277.4,1265.9,1268.3,1103.9,1209.0,1273.1,1290.0,1249.1,1243.2,1191.2],
  "London, England, UK": [799.9,737.8,781.5,728.0,786.3,788.1,757.5,808.9,846.6,804.4,819.7,750.0,804.8,734.7,809.5,822.1,822.5,792.9,814.5,800.2,757.5,821.0,781.0,748.7],
};

const monthlyRainPct: Record<string, number[]> = {
  "Mt. Waialeale, Kauai, HI, USA": [81.6,80.6,81.4,81.2,81.9,82.8,82.8,82.2,82.4,81.4,81.2,81.7,80.4,81.4,82.4,80.2,83.4,81.9,81.0,82.2,81.7,81.6,81.7,82.0],
  "Bergen, Vestland, Norway": [56.0,53.7,53.7,54.2,54.7,53.7,55.1,54.8,52.9,54.8,55.4,55.0,56.0,55.3,55.1,54.5,54.9,53.9,54.6,55.8,54.7,55.0,53.5,56.2],
  "Singapore": [50.5,48.9,49.9,49.5,49.8,51.4,51.2,50.0,50.8,50.8,48.6,49.4,50.3,50.8,50.4,49.5,50.6,50.9,50.3,50.0,49.6,51.9,50.6,49.7],
  "Seattle, WA, USA": [42.8,41.9,42.1,43.2,42.2,42.5,43.1,42.2,43.2,42.5,42.3,43.3,39.7,44.2,40.9,42.0,41.5,42.0,41.1,43.6,42.7,43.1,42.3,44.1],
  "Tokyo, Japan": [34.8,36.2,35.3,35.9,35.8,35.3,35.4,36.6,34.7,35.9,34.6,34.4,34.1,35.1,34.9,36.5,34.3,33.0,33.8,35.1,37.7,35.5,35.1,34.7],
  "London, England, UK": [38.1,37.0,36.8,37.2,37.7,39.5,37.5,38.7,40.6,38.3,38.1,36.4,36.7,38.7,38.3,39.1,38.2,38.2,38.6,38.1,38.5,38.5,38.4,36.8],
};

const intensityData = [
  { label: "Moderate", value: 97378 },
  { label: "Light", value: 82224 },
  { label: "Heavy", value: 75274 },
];

const totalPrecip = locations.reduce((s, l) => s + l.precip, 0);
const avgTemp = locations.reduce((s, l) => s + l.temp, 0) / locations.length;
const avgHumidity = locations.reduce((s, l) => s + l.humidity, 0) / locations.length;
const maxWindGlobal = Math.max(...locations.map((l) => l.maxWind));

type View = "overview" | "precipitation" | "climate" | "table";

export default function RainSensorDashboard() {
  const { tokens: t } = useHostTheme();
  const [view, setView] = useCanvasState<View>("view", "overview");

  const views: { id: View; label: string }[] = [
    { id: "overview", label: "Overview" },
    { id: "precipitation", label: "Precipitation" },
    { id: "climate", label: "Climate" },
    { id: "table", label: "All Data" },
  ];

  return (
    <Stack gap={24}>
      <Stack gap={8}>
        <H1>Rain Sensor Network Dashboard</H1>
        <Text tone="secondary">
          Hourly weather summaries from 20 global sensor locations — Jan 2024 to Dec 2025 — 350,000 aggregated readings
        </Text>
      </Stack>

      <Row gap={8}>
        {views.map((v) => (
          <Pill key={v.id} active={view === v.id} onClick={() => setView(v.id)}>
            {v.label}
          </Pill>
        ))}
      </Row>

      {view === "overview" && <OverviewView t={t} />}
      {view === "precipitation" && <PrecipitationView />}
      {view === "climate" && <ClimateView t={t} />}
      {view === "table" && <TableView />}
    </Stack>
  );
}

function OverviewView({ t }: { t: any }) {
  return (
    <Stack gap={20}>
      <Grid columns={4} gap={16}>
        <Stat value="20" label="Sensor Locations" />
        <Stat value={`${(totalPrecip / 1000).toFixed(0)} m`} label="Total Precipitation" />
        <Stat value={`${avgTemp.toFixed(1)}°C`} label="Avg Temperature" />
        <Stat value={`${maxWindGlobal} km/h`} label="Peak Wind Speed" tone="warning" />
      </Grid>

      <Divider />

      <H2>Total Precipitation by Location</H2>
      <BarChart
        categories={locations.map((l) => l.name)}
        series={[{ name: "Precipitation (mm)", data: locations.map((l) => Math.round(l.precip)) }]}
        horizontal
        height={500}
        valueSuffix=" mm"
      />

      <Divider />

      <Grid columns={2} gap={20}>
        <Stack gap={12}>
          <H3>Rain Intensity Distribution</H3>
          <Text tone="secondary" size="small">Across all hourly readings with rain</Text>
          <PieChart data={intensityData} donut size={200} />
        </Stack>

        <Stack gap={12}>
          <H3>Wettest Locations</H3>
          <Text tone="secondary" size="small">Top 5 by total precipitation</Text>
          <Table
            headers={["Location", "Precip (mm)", "Rain %"]}
            rows={locations.slice(0, 5).map((l) => [
              l.name,
              l.precip.toLocaleString(),
              `${l.rainPct}%`,
            ])}
            columnAlign={["left", "right", "right"]}
          />
        </Stack>
      </Grid>
    </Stack>
  );
}

function PrecipitationView() {
  const precipSeries = [
    "Mt. Waialeale, Kauai, HI, USA",
    "Mawsynram, Meghalaya, India",
    "Cherrapunji, Meghalaya, India",
    "Singapore",
    "Bergen, Vestland, Norway",
  ];

  const citySeries = [
    "Seattle, WA, USA",
    "Tokyo, Japan",
    "London, England, UK",
  ];

  return (
    <Stack gap={20}>
      <H2>Monthly Precipitation — Wettest Locations</H2>
      <Text tone="secondary" size="small">Total monthly precipitation in mm across 2024-2025</Text>
      <LineChart
        categories={months}
        series={precipSeries.map((name) => ({
          name: name.split(",")[0],
          data: monthlyPrecip[name],
        }))}
        height={320}
        fill
        valueSuffix=" mm"
      />

      <Divider />

      <H2>Monthly Precipitation — Major Cities</H2>
      <Text tone="secondary" size="small">Tokyo, London, and Seattle compared over 24 months</Text>
      <LineChart
        categories={months}
        series={citySeries.map((name) => ({
          name: name.split(",")[0],
          data: monthlyPrecip[name],
        }))}
        height={280}
        fill
        valueSuffix=" mm"
      />

      <Divider />

      <H2>Driest Locations</H2>
      <Table
        headers={["Location", "Total Precip (mm)", "Rain %", "Avg Humidity"]}
        rows={locations.slice(-3).reverse().map((l) => [
          l.name,
          l.precip.toLocaleString(),
          `${l.rainPct}%`,
          `${l.humidity}%`,
        ])}
        columnAlign={["left", "right", "right", "right"]}
      />
    </Stack>
  );
}

function ClimateView({ t }: { t: any }) {
  const rainPctSeries = [
    "Mt. Waialeale, Kauai, HI, USA",
    "Bergen, Vestland, Norway",
    "Singapore",
    "Seattle, WA, USA",
    "Tokyo, Japan",
    "London, England, UK",
  ];

  return (
    <Stack gap={20}>
      <H2>Rain Frequency Over Time</H2>
      <Text tone="secondary" size="small">Percentage of hourly readings with active rain</Text>
      <LineChart
        categories={months}
        series={rainPctSeries.map((name) => ({
          name: name.split(",")[0],
          data: monthlyRainPct[name],
        }))}
        height={320}
        valueSuffix="%"
      />

      <Divider />

      <H2>Temperature vs Humidity</H2>
      <Grid columns={2} gap={16}>
        <Stack gap={8}>
          <H3>Warmest</H3>
          <Table
            headers={["Location", "Avg Temp", "Humidity"]}
            rows={[...locations]
              .sort((a, b) => b.temp - a.temp)
              .slice(0, 5)
              .map((l) => [l.name, `${l.temp}°C`, `${l.humidity}%`])}
            columnAlign={["left", "right", "right"]}
          />
        </Stack>
        <Stack gap={8}>
          <H3>Coldest</H3>
          <Table
            headers={["Location", "Avg Temp", "Humidity"]}
            rows={[...locations]
              .sort((a, b) => a.temp - b.temp)
              .slice(0, 5)
              .map((l) => [l.name, `${l.temp}°C`, `${l.humidity}%`])}
            columnAlign={["left", "right", "right"]}
          />
        </Stack>
      </Grid>

      <Divider />

      <H2>Wind Conditions</H2>
      <BarChart
        categories={locations.map((l) => l.name)}
        series={[
          { name: "Avg Wind (km/h)", data: locations.map((l) => l.wind) },
          { name: "Max Wind (km/h)", data: locations.map((l) => l.maxWind) },
        ]}
        horizontal
        height={500}
        valueSuffix=" km/h"
      />
    </Stack>
  );
}

function TableView() {
  return (
    <Stack gap={16}>
      <H2>All Locations</H2>
      <Text tone="secondary" size="small">Aggregated across the full 2-year period per location</Text>
      <Table
        headers={[
          "Location",
          "Total Precip",
          "Avg Temp",
          "Humidity",
          "Avg Wind",
          "Max Wind",
          "Rain %",
        ]}
        rows={locations.map((l) => [
          l.name,
          `${l.precip.toLocaleString()} mm`,
          `${l.temp}°C`,
          `${l.humidity}%`,
          `${l.wind} km/h`,
          `${l.maxWind} km/h`,
          `${l.rainPct}%`,
        ])}
        columnAlign={["left", "right", "right", "right", "right", "right", "right"]}
        striped
        stickyHeader
      />
    </Stack>
  );
}
