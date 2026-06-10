import Svg, { Circle, G, Path } from "react-native-svg";

import { colors } from "../../theme/colors";

const DAILY_FOOD_TIP_BULB_PATH = [
  "M30.15 14.77 L26.87 15.53 L22.95 17.71 L20.88 19.68 L19.13 22.29 L17.93 25.13 L17.5 27.09 L17.5 30.91 L17.93 33.09 L19.35 36.47 L23.82 41.71 L24.04 45.09 L24.58 47.05 L25.57 48.14 L26.98 48.9 L26.11 50.21 L26.22 51.52 L26.98 52.39 L26.11 53.7 L26.33 55.12 L26.77 55.66 L27.97 56.1 L28.29 57.19 L29.71 58.17 L33.74 58.39 L35.27 57.74 L36.14 56.21 L37.23 55.77 L38 54.79 L38 53.37 L37.23 52.5 L38 51.3 L38 49.88 L37.23 48.9 L38.43 48.36 L39.52 47.16 L40.07 45.63 L40.4 41.71 L43.78 38 L45.41 34.94 L46.4 30.91 L46.4 27.2 L45.96 25.02 L44.87 22.4 L43.01 19.68 L41.16 17.93 L37.02 15.53 L33.74 14.77 Z",
  "M29.6 18.7 L34.51 18.7 L38.22 20.44 L41.27 23.49 L42.58 26.66 L42.8 30.15 L41.92 33.74 L40.94 35.49 L37.02 39.96 L36.47 41.27 L36.25 45.3 L35.93 45.63 L27.97 45.63 L27.64 45.3 L27.53 42.14 L27.09 40.29 L22.62 34.94 L21.53 32 L21.31 27.42 L22.95 23.28 L25.67 20.44 Z",
  "M15.97 11.72 L14.88 12.81 L14.88 14.55 L19.79 19.46 L22.62 16.41 L18.26 11.93 L17.6 11.61 Z",
  "M8.77 27.53 L8.77 29.27 L10.08 30.47 L16.73 30.47 L16.95 26.33 L10.08 26.33 Z",
  "M47.92 11.72 L46.29 11.61 L45.63 11.93 L41.27 16.19 L44 19.24 L48.79 14.77 L49.12 14.01 L49.01 12.81 Z",
  "M31.13 5.61 L30.15 6.37 L29.82 7.24 L29.82 13.24 L33.96 13.35 L33.96 6.81 L32.76 5.61 Z",
  "M47.05 26.33 L47.38 30.36 L53.59 30.36 L54.57 29.93 L55.12 29.16 L55.12 27.53 L53.81 26.33 Z",
  "M44.76 38.11 L42.25 41.38 L45.96 44.98 L48.03 44.98 L49.01 44 L49.01 42.03 Z",
  "M19.13 38.11 L14.88 42.25 L14.88 43.67 L15.97 44.98 L18.04 44.98 L21.75 41.38 Z",
].join(" ");

const CENTERED_ICON_TRANSFORM = "translate(32 32) scale(0.7) translate(-32 -32)";

type DailyFoodTipIconProps = {
  backgroundColor?: string;
  color?: string;
  height?: number;
  width?: number;
};

export function DailyFoodTipIcon({
  backgroundColor = colors.accent,
  color = "#FFF8E8",
  height = 58,
  width = 58,
}: DailyFoodTipIconProps) {
  return (
    <Svg height={height} viewBox="0 0 64 64" width={width}>
      <Circle cx="32" cy="32" fill={backgroundColor} r="30" />
      <G transform={CENTERED_ICON_TRANSFORM}>
        <Path d={DAILY_FOOD_TIP_BULB_PATH} fill={color} fillRule="evenodd" />
      </G>
    </Svg>
  );
}
