<?php
declare(strict_types=1);

header("Content-Type: application/json; charset=utf-8");
header("Cache-Control: no-store, no-cache, must-revalidate, max-age=0");

if (($_SERVER["REQUEST_METHOD"] ?? "GET") !== "GET") {
    http_response_code(405);
    echo json_encode(["error" => "Method Not Allowed"], JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
    exit;
}

const BENCHMARK = "SPY";
const TRAIL_LENGTH = 5;
const RS_PERIOD = 10;

$sectorEtfs = [
    ["name" => "Technology", "symbol" => "XLK"],
    ["name" => "Health Care", "symbol" => "XLV"],
    ["name" => "Financials", "symbol" => "XLF"],
    ["name" => "Energy", "symbol" => "XLE"],
    ["name" => "Consumer Discretionary", "symbol" => "XLY"],
    ["name" => "Consumer Staples", "symbol" => "XLP"],
    ["name" => "Industrials", "symbol" => "XLI"],
    ["name" => "Materials", "symbol" => "XLB"],
    ["name" => "Utilities", "symbol" => "XLU"],
    ["name" => "Real Estate", "symbol" => "XLRE"],
    ["name" => "Communication Services", "symbol" => "XLC"],
];

try {
    $config = loadRrgConfig();
    $apiKey = resolveApiKey($config);

    if ($apiKey === "") {
        throw new RuntimeException("RRG server API key is not configured. Set ALPHA_VANTAGE_API_KEY or create public/api/rrg-config.php.");
    }

    $responseTtlSeconds = (int)($config["response_ttl_seconds"] ?? 21600);
    $symbolTtlSeconds = (int)($config["symbol_ttl_seconds"] ?? 21600);
    $delaySeconds = (int)($config["request_delay_seconds"] ?? 13);
    $cacheDir = resolveCacheDir();
    $responseCacheFile = $cacheDir . DIRECTORY_SEPARATOR . "rrg-response.json";
    $cachedResponse = loadJsonFile($responseCacheFile);

    if (isFreshPayload($cachedResponse, $responseTtlSeconds)) {
        respondJson(200, $cachedResponse["payload"]);
    }

    $remoteFetchCount = 0;
    $benchmarkPrices = loadSymbolPrices(BENCHMARK, $apiKey, $cacheDir, $symbolTtlSeconds, $delaySeconds, $remoteFetchCount);
    $results = [];
    $warnings = [];

    foreach ($sectorEtfs as $etf) {
        try {
            $sectorPrices = loadSymbolPrices($etf["symbol"], $apiKey, $cacheDir, $symbolTtlSeconds, $delaySeconds, $remoteFetchCount);
            $trail = calculateRrgTrail($sectorPrices, $benchmarkPrices, RS_PERIOD, TRAIL_LENGTH);

            if ($trail !== []) {
                $results[] = [
                    "name" => $etf["name"],
                    "symbol" => $etf["symbol"],
                    "trail" => $trail,
                ];
            }
        } catch (Throwable $error) {
            $warnings[] = $etf["symbol"] . ": " . $error->getMessage();
        }
    }

    if ($results === []) {
        throw new RuntimeException("No RRG sector data could be calculated. " . implode(" | ", $warnings));
    }

    $payload = [
        "sectors" => $results,
        "lastUpdated" => gmdate(DATE_ATOM),
    ];

    if ($warnings !== []) {
        $payload["warnings"] = $warnings;
    }

    writeJsonFile($responseCacheFile, [
        "cachedAt" => time(),
        "payload" => $payload,
    ]);

    respondJson(200, $payload);
} catch (Throwable $error) {
    $cacheDir = isset($cacheDir) ? $cacheDir : resolveCacheDir();
    $cachedResponse = isset($cachedResponse) ? $cachedResponse : loadJsonFile($cacheDir . DIRECTORY_SEPARATOR . "rrg-response.json");

    if (is_array($cachedResponse["payload"] ?? null)) {
        $stalePayload = $cachedResponse["payload"];
        $stalePayload["warning"] = $error->getMessage();
        $stalePayload["stale"] = true;
        respondJson(200, $stalePayload);
    }

    respondJson(500, [
        "error" => $error->getMessage(),
    ]);
}

function loadRrgConfig(): array
{
    $configPath = __DIR__ . DIRECTORY_SEPARATOR . "rrg-config.php";
    if (!is_file($configPath)) {
        return [];
    }

    $config = require $configPath;
    return is_array($config) ? $config : [];
}

function resolveApiKey(array $config): string
{
    $envKey = trim((string)getenv("ALPHA_VANTAGE_API_KEY"));
    if ($envKey !== "") {
        return $envKey;
    }

    return trim((string)($config["alpha_vantage_api_key"] ?? ""));
}

function resolveCacheDir(): string
{
    $candidateDirs = [
        __DIR__ . DIRECTORY_SEPARATOR . ".cache",
        rtrim(sys_get_temp_dir(), DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . "market-pulse-rrg",
    ];

    foreach ($candidateDirs as $dir) {
        if ((is_dir($dir) || @mkdir($dir, 0775, true)) && is_writable($dir)) {
            return $dir;
        }
    }

    throw new RuntimeException("Unable to create a writable cache directory for RRG data.");
}

function cacheFilePath(string $cacheDir, string $symbol): string
{
    return $cacheDir . DIRECTORY_SEPARATOR . strtolower(preg_replace("/[^a-z0-9]+/i", "-", $symbol)) . ".json";
}

function loadSymbolPrices(string $symbol, string $apiKey, string $cacheDir, int $ttlSeconds, int $delaySeconds, int &$remoteFetchCount): array
{
    $cacheFile = cacheFilePath($cacheDir, $symbol);
    $cached = loadJsonFile($cacheFile);

    if (isFreshPayload($cached, $ttlSeconds) && is_array($cached["payload"]["prices"] ?? null)) {
        return $cached["payload"]["prices"];
    }

    if ($remoteFetchCount > 0 && $delaySeconds > 0) {
        sleep($delaySeconds);
    }

    $url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&outputsize=compact&symbol=" . rawurlencode($symbol) . "&apikey=" . rawurlencode($apiKey);
    $response = httpGetJson($url);
    $remoteFetchCount++;

    if (!empty($response["Note"]) || !empty($response["Information"])) {
        throw new RuntimeException("Alpha Vantage rate limit was reached.");
    }

    $timeSeries = $response["Time Series (Daily)"] ?? null;
    if (!is_array($timeSeries)) {
        throw new RuntimeException("Missing daily time series for " . $symbol . ".");
    }

    ksort($timeSeries);
    $prices = [];

    foreach ($timeSeries as $row) {
        if (!is_array($row) || !isset($row["4. close"])) {
            continue;
        }

        $prices[] = (float)$row["4. close"];
    }

    if ($prices === []) {
        throw new RuntimeException("No closing prices were returned for " . $symbol . ".");
    }

    writeJsonFile($cacheFile, [
        "cachedAt" => time(),
        "payload" => [
            "symbol" => $symbol,
            "prices" => $prices,
        ],
    ]);

    return $prices;
}

function calculateRrgTrail(array $sectorPrices, array $benchmarkPrices, int $period, int $trailLength): array
{
    $length = min(count($sectorPrices), count($benchmarkPrices));
    if ($length < ($period + $trailLength + 5)) {
        return [];
    }

    $rs = [];
    for ($index = 0; $index < $length; $index++) {
        if ((float)$benchmarkPrices[$index] === 0.0) {
            continue;
        }

        $rs[] = (float)$sectorPrices[$index] / (float)$benchmarkPrices[$index];
    }

    $rsAverage = [];
    for ($index = $period - 1; $index < count($rs); $index++) {
        $slice = array_slice($rs, $index - $period + 1, $period);
        $rsAverage[] = array_sum($slice) / $period;
    }

    $rsRatio = [];
    foreach ($rsAverage as $index => $average) {
        if ($average === 0.0) {
            continue;
        }

        $rawIndex = $index + $period - 1;
        $rsRatio[] = ($rs[$rawIndex] / $average) * 100;
    }

    $trail = [];
    $startIndex = count($rsRatio) - $trailLength;

    for ($index = 0; $index < $trailLength; $index++) {
        $ratioIndex = $startIndex + $index;
        if ($ratioIndex < 1 || !isset($rsRatio[$ratioIndex], $rsRatio[$ratioIndex - 1]) || $rsRatio[$ratioIndex - 1] == 0.0) {
            continue;
        }

        $ratio = $rsRatio[$ratioIndex];
        $momentum = ($ratio / $rsRatio[$ratioIndex - 1]) * 100;
        $trail[] = [
            "rsRatio" => round($ratio, 2),
            "rsMomentum" => round($momentum, 2),
        ];
    }

    return $trail;
}

function httpGetJson(string $url): array
{
    if (!function_exists("curl_init")) {
        $content = @file_get_contents($url);
        if ($content === false) {
            throw new RuntimeException("HTTP request failed.");
        }

        $decoded = json_decode($content, true);
        if (!is_array($decoded)) {
            throw new RuntimeException("Failed to decode JSON response.");
        }

        return $decoded;
    }

    $curl = curl_init($url);
    curl_setopt_array($curl, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_FOLLOWLOCATION => true,
        CURLOPT_CONNECTTIMEOUT => 10,
        CURLOPT_TIMEOUT => 30,
        CURLOPT_HTTPHEADER => [
            "Accept: application/json",
            "User-Agent: MarketPulseRRG/1.0",
        ],
    ]);

    $response = curl_exec($curl);
    $statusCode = (int)curl_getinfo($curl, CURLINFO_HTTP_CODE);
    $error = curl_error($curl);
    curl_close($curl);

    if ($response === false || $statusCode >= 400) {
        throw new RuntimeException($error !== "" ? $error : "HTTP request failed with status " . $statusCode . ".");
    }

    $decoded = json_decode($response, true);
    if (!is_array($decoded)) {
        throw new RuntimeException("Failed to decode JSON response.");
    }

    return $decoded;
}

function loadJsonFile(string $path): array
{
    if (!is_file($path)) {
        return [];
    }

    $decoded = json_decode((string)file_get_contents($path), true);
    return is_array($decoded) ? $decoded : [];
}

function writeJsonFile(string $path, array $payload): void
{
    file_put_contents($path, json_encode($payload, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE), LOCK_EX);
}

function isFreshPayload(array $cached, int $ttlSeconds): bool
{
    $cachedAt = (int)($cached["cachedAt"] ?? 0);
    return $cachedAt > 0 && (time() - $cachedAt) < $ttlSeconds;
}

function respondJson(int $statusCode, array $payload): void
{
    http_response_code($statusCode);
    echo json_encode($payload, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
    exit;
}
