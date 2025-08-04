import util from "util";
import fs from "fs";
import path from "path";
import dotenv from "dotenv";
import { getProvider } from "./connection";
import { fetchV2Stats } from "./v2/fetchV2Stats";
import v2MainnetDeployment from "./v2MainnetDeployment.json";
import { DUNE_SPV2_AVERAGE_APY_URL_MAINNET, OUTPUT_DIR_V2 } from "./constants";

// Load environment variables from .env file
dotenv.config();

const panic = <T>(message: string): T => {
  throw new Error(message);
};

const alchemyApiKey = process.env.ALCHEMY_API_KEY || undefined; // filter out empty string
const duneApiKey: string = process.env.DUNE_API_KEY || ""; // Dune is deprecated, empty key will be handled gracefully

const mainnetProvider = getProvider("mainnet", { alchemyApiKey });

async function generateV2Stats() {
  try {
    const v2MainnetStats = await fetchV2Stats({
      deployment: v2MainnetDeployment,
      duneApiKey,
      provider: mainnetProvider,
      duneUrl: DUNE_SPV2_AVERAGE_APY_URL_MAINNET
    });
    // Ensure output directory exists
    fs.mkdirSync(OUTPUT_DIR_V2, { recursive: true });

    // Write mainnet stats to JSON file
    fs.writeFileSync(
      path.join(OUTPUT_DIR_V2, "mainnet.json"),
      JSON.stringify(v2MainnetStats, null, 2)
    );

    console.log("v2 mainnet stats:", util.inspect(v2MainnetStats, { colors: true, depth: null }));
  } catch (error) {
    console.error(error);
    process.exit(1);
  }
}

generateV2Stats();
