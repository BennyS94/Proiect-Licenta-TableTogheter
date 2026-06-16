import Svg, { Path } from "react-native-svg";

import { colors } from "../../theme/colors";

type ChevronDownIconProps = {
  color?: string;
  size?: number;
  strokeWidth?: number;
};

export function ChevronDownIcon({
  color = colors.accent,
  size = 16,
  strokeWidth = 2.2,
}: ChevronDownIconProps) {
  return (
    <Svg fill="none" height={size} viewBox="0 0 24 24" width={size}>
      <Path
        d="M6 9l6 6 6-6"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth={strokeWidth}
      />
    </Svg>
  );
}
