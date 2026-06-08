import Svg, { Circle, Line, Path } from "react-native-svg";

type MealTimeIconProps = {
  size?: number;
};

const STROKE = "#4F8F1F";

export function BreakfastSunriseIcon({ size = 24 }: MealTimeIconProps) {
  return (
    <Svg height={size} viewBox="0 0 48 48" width={size}>
      <Path
        d="M12 31c1.5-5.2 6.2-9 12-9s10.5 3.8 12 9"
        fill="none"
        stroke={STROKE}
        strokeLinecap="round"
        strokeWidth="4"
      />
      <Line stroke={STROKE} strokeLinecap="round" strokeWidth="4" x1="7" x2="41" y1="35" y2="35" />
      <Line stroke={STROKE} strokeLinecap="round" strokeWidth="4" x1="24" x2="24" y1="9" y2="15" />
      <Line stroke={STROKE} strokeLinecap="round" strokeWidth="4" x1="10.5" x2="15" y1="16" y2="20" />
      <Line stroke={STROKE} strokeLinecap="round" strokeWidth="4" x1="37.5" x2="33" y1="16" y2="20" />
    </Svg>
  );
}

export function LunchSunIcon({ size = 24 }: MealTimeIconProps) {
  return (
    <Svg height={size} viewBox="0 0 48 48" width={size}>
      <Circle cx="24" cy="24" fill="none" r="8.5" stroke={STROKE} strokeWidth="4" />
      <Line stroke={STROKE} strokeLinecap="round" strokeWidth="4" x1="24" x2="24" y1="6" y2="11" />
      <Line stroke={STROKE} strokeLinecap="round" strokeWidth="4" x1="24" x2="24" y1="37" y2="42" />
      <Line stroke={STROKE} strokeLinecap="round" strokeWidth="4" x1="6" x2="11" y1="24" y2="24" />
      <Line stroke={STROKE} strokeLinecap="round" strokeWidth="4" x1="37" x2="42" y1="24" y2="24" />
      <Line stroke={STROKE} strokeLinecap="round" strokeWidth="4" x1="11.5" x2="15" y1="11.5" y2="15" />
      <Line stroke={STROKE} strokeLinecap="round" strokeWidth="4" x1="36.5" x2="33" y1="11.5" y2="15" />
      <Line stroke={STROKE} strokeLinecap="round" strokeWidth="4" x1="11.5" x2="15" y1="36.5" y2="33" />
      <Line stroke={STROKE} strokeLinecap="round" strokeWidth="4" x1="36.5" x2="33" y1="36.5" y2="33" />
    </Svg>
  );
}

export function DinnerMoonIcon({ size = 24 }: MealTimeIconProps) {
  return (
    <Svg height={size} viewBox="0 0 48 48" width={size}>
      <Path
        d="M32.5 37.2c-10.7 2.1-20.1-6-20.1-16.5 0-5.2 2.4-10 6.3-13.1-.6 7.3 4.8 14.4 12.5 15.8 2.6.5 5.1.3 7.4-.5-1 7.4-3.4 12.3-6.1 14.3Z"
        fill="none"
        stroke={STROKE}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="4"
      />
    </Svg>
  );
}

