import { Image } from "react-native";
import Svg, { Path } from "react-native-svg";

export type MacroNutrientIconProps = {
  color?: string;
  highlightColor?: string;
  size?: number;
};

const proteinDrumstickAsset = require("../../../assets/insights/macro/protein_drumstick.png");
const carbsWheatAsset = require("../../../assets/insights/macro/carbs_wheat.png");
const fatsAvocadoAsset = require("../../../assets/insights/macro/fats_avocado.png");

export function CaloriesFlameIcon({ size = 32 }: MacroNutrientIconProps) {
  return (
    <Svg height={size} viewBox="0 0 64 64" width={size}>
      <Path
        d="M34 7c6.8 7.2 12.8 14.2 11.7 24.2 3-2.1 4.7-5 5.2-8.8 7.2 9.3 5.9 23.8-3.8 31.1-9.1 6.9-24 5.6-31.3-3.8C7.2 38.5 12 24 25.8 14.6c4.1-2.8 6.6-5.3 8.2-7.6Z"
        fill="#1F252D"
      />
      <Path
        d="M33.2 36.5c4.3 4.3 7.9 8.3 7.5 13.1-.5 5.7-5 8.9-9.6 8.9-5.3 0-9.8-3.9-9.8-9.4 0-5.3 3.8-9 8.5-12.5 1.3-1 2.5-2 3.4-3.2.3 1 .3 2 .1 3.1Z"
        fill="#FFFFFF"
      />
    </Svg>
  );
}

export function ProteinDrumstickIcon({
  size = 32,
}: MacroNutrientIconProps) {
  return (
    <Image
      accessibilityIgnoresInvertColors
      resizeMode="contain"
      source={proteinDrumstickAsset}
      style={{ height: size, width: size }}
    />
  );
}

export function CarbsWheatIcon({ size = 32 }: MacroNutrientIconProps) {
  return (
    <Image
      accessibilityIgnoresInvertColors
      resizeMode="contain"
      source={carbsWheatAsset}
      style={{ height: size, width: size }}
    />
  );
}

export function FatsAvocadoIcon({ size = 32 }: MacroNutrientIconProps) {
  return (
    <Image
      accessibilityIgnoresInvertColors
      resizeMode="contain"
      source={fatsAvocadoAsset}
      style={{ height: size, width: size }}
    />
  );
}
