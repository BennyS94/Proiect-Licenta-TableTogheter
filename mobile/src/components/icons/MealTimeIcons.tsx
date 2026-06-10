import Svg, { Path } from "react-native-svg";

import { colors } from "../../theme/colors";
import {
  BREAKFAST_COFFEE_PATH,
  DINNER_PLATE_CUTLERY_PATH,
  LUNCH_SERVING_DOME_PATH,
  SNACK_APPLE_PATH,
} from "./mealTimeIconPaths";

export type MealTimeIconProps = {
  color?: string;
  size?: number;
};

const DEFAULT_MEAL_ICON_COLOR = colors.accent;

function MealPathIcon({
  color = DEFAULT_MEAL_ICON_COLOR,
  path,
  size = 24,
}: MealTimeIconProps & { path: string }) {
  return (
    <Svg height={size} viewBox="0 0 128 128" width={size}>
      <Path d={path} fill={color} fillRule="evenodd" />
    </Svg>
  );
}

export function BreakfastCoffeeCupIcon(props: MealTimeIconProps) {
  return <MealPathIcon {...props} path={BREAKFAST_COFFEE_PATH} />;
}

export function LunchServingDomeIcon(props: MealTimeIconProps) {
  return <MealPathIcon {...props} path={LUNCH_SERVING_DOME_PATH} />;
}

export function SnackAppleIcon(props: MealTimeIconProps) {
  return <MealPathIcon {...props} path={SNACK_APPLE_PATH} />;
}

export function DinnerPlateCutleryIcon(props: MealTimeIconProps) {
  return <MealPathIcon {...props} path={DINNER_PLATE_CUTLERY_PATH} />;
}
