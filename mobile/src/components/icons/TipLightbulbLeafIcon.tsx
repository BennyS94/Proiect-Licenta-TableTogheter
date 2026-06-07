import Svg, { Circle, Ellipse, Path, Rect } from "react-native-svg";

type TipLightbulbLeafIconProps = {
  size?: number;
};

export function TipLightbulbLeafIcon({ size = 56 }: TipLightbulbLeafIconProps) {
  return (
    <Svg height={size} viewBox="0 0 64 64" width={size}>
      <Circle cx="32" cy="32" fill="#74B72E" opacity="0.24" r="29" />
      <Path
        d="M23 29.4C23 21.9 28.7 16 36 16c6.6 0 12 5 12 11.4 0 4.1-2 7.3-5 9.7-1.8 1.5-2.8 3.4-2.8 5.7v1.5H28.8v-1.5c0-2.3-1-4.3-2.8-5.8-1.9-1.7-3-4.3-3-7.6Z"
        fill="#FFF2B8"
      />
      <Path
        d="M26.5 29.2c0-5.7 4.2-10.2 9.6-10.2 4.9 0 8.9 3.8 8.9 8.7 0 3.1-1.5 5.4-3.7 7.2-2.2 1.8-3.6 4.2-3.7 7.2h-6c-.1-3-1.5-5.5-3.8-7.4-1.2-1.1-1.3-3.1-1.3-5.5Z"
        fill="#FFFBEA"
        opacity="0.86"
      />
      <Path
        d="M39.7 20.6c6.1 1.2 10.4 6 10.4 11.7 0 2.9-1.1 5.5-3 7.5 3.2-1.8 5.2-5.1 5.2-8.9 0-5.9-5-10.6-11.2-10.6-.5 0-1 0-1.4.3Z"
        fill="#FFFFFF"
        opacity="0.55"
      />
      <Path
        d="M43 24c-6.7-.3-11.1 3.4-12.5 9.5 6.6.6 11.4-3.5 12.5-9.5Z"
        fill="#74B72E"
      />
      <Path
        d="M42 25.1c-3.7 1-6.5 3.1-8.7 6.3"
        fill="none"
        stroke="#4F8F1F"
        strokeLinecap="round"
        strokeWidth="2"
      />
      <Ellipse cx="24.2" cy="33.5" fill="#A9D76D" rx="7.4" ry="5.4" transform="rotate(31 24.2 33.5)" />
      <Path
        d="M19.3 31.3c4.2 1 7.2 2.8 9.6 5.4"
        fill="none"
        stroke="#6FAF2A"
        strokeLinecap="round"
        strokeWidth="1.7"
      />
      <Rect fill="#A7CEC5" height="4.6" rx="2.3" width="16" x="24" y="44" />
      <Rect fill="#7EB5AA" height="4.6" rx="2.3" width="13" x="25.5" y="49" />
      <Path
        d="M29.5 54h5"
        fill="none"
        stroke="#4F8F1F"
        strokeLinecap="round"
        strokeWidth="2.4"
      />
    </Svg>
  );
}
