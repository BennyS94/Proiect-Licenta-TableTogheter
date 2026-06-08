import type { ReactNode } from "react";
import { StyleSheet, View } from "react-native";
import Svg, { Circle, Path } from "react-native-svg";

import { CaloriesFlameIcon } from "../icons/MacroNutrientIcons";

type MacroDonutRingProps = {
  carbsPercent: number;
  fatPercent: number;
  proteinPercent: number;
  size?: number;
};

const PROTEIN_COLOR = "#EF3B45";
const CARBS_COLOR = "#F28A12";
const FAT_COLOR = "#F5C400";
const SEPARATOR_PERCENT = 0.008;

export function MacroDonutRing({
  carbsPercent,
  fatPercent,
  proteinPercent,
  size = 140,
}: MacroDonutRingProps) {
  const center = size / 2;
  const outerRadius = center - 8;
  const innerRadius = center - Math.round(size * 0.3);
  const segments = normalizeSegments([
    { color: PROTEIN_COLOR, percent: proteinPercent },
    { color: CARBS_COLOR, percent: carbsPercent },
    { color: FAT_COLOR, percent: fatPercent },
  ]);

  return (
    <View style={[styles.shell, { height: size, width: size }]}>
      <Svg height={size} style={StyleSheet.absoluteFill} width={size}>
        <Circle
          cx={center}
          cy={center}
          fill="#FFFFFF"
          r={innerRadius}
        />
        <Circle cx={center} cy={center} fill="none" r={(outerRadius + innerRadius) / 2} />
        {segments.length === 1 ? (
          <Circle
            cx={center}
            cy={center}
            fill="none"
            r={(outerRadius + innerRadius) / 2}
            stroke={segments[0].color}
            strokeLinecap="round"
            strokeWidth={outerRadius - innerRadius}
          />
        ) : (
          renderArcSegments(segments, center, outerRadius, innerRadius)
        )}
      </Svg>
      <View style={styles.centerIcon}>
        <CaloriesFlameIcon size={46} />
      </View>
    </View>
  );
}

function renderArcSegments(
  segments: Array<{ color: string; percent: number }>,
  center: number,
  outerRadius: number,
  innerRadius: number,
) {
  const gapDegrees = 360 * SEPARATOR_PERCENT;
  const availableDegrees = 360 - gapDegrees * segments.length;
  let cursor = -106;
  const colorSegments: ReactNode[] = [];

  segments.forEach((segment, index) => {
    const span = (segment.percent / 100) * availableDegrees;
    const start = cursor;
    const end = cursor + span;
    cursor = end + gapDegrees;

    if (end > start) {
      colorSegments.push(
        <Path
          d={ringSegmentPath(center, center, outerRadius, innerRadius, start, end)}
          fill={segment.color}
          key={`macro-${segment.color}-${index}`}
        />,
      );
    }
  });

  return colorSegments;
}

function normalizeSegments(segments: Array<{ color: string; percent: number }>) {
  const safeSegments = segments
    .map((segment) => ({
      color: segment.color,
      percent: Number.isFinite(segment.percent) ? Math.max(0, segment.percent) : 0,
    }))
    .filter((segment) => segment.percent > 0.2);
  const total = safeSegments.reduce((sum, segment) => sum + segment.percent, 0);
  if (total <= 0) {
    return [];
  }
  return safeSegments.map((segment) => ({
    ...segment,
    percent: (segment.percent / total) * 100,
  }));
}

function ringSegmentPath(
  centerX: number,
  centerY: number,
  outerRadius: number,
  innerRadius: number,
  startAngle: number,
  endAngle: number,
) {
  const outerStart = polarToCartesian(centerX, centerY, outerRadius, startAngle);
  const outerEnd = polarToCartesian(centerX, centerY, outerRadius, endAngle);
  const innerEnd = polarToCartesian(centerX, centerY, innerRadius, endAngle);
  const innerStart = polarToCartesian(centerX, centerY, innerRadius, startAngle);
  const largeArcFlag = endAngle - startAngle <= 180 ? "0" : "1";
  return [
    `M ${outerStart.x} ${outerStart.y}`,
    `A ${outerRadius} ${outerRadius} 0 ${largeArcFlag} 1 ${outerEnd.x} ${outerEnd.y}`,
    `L ${innerEnd.x} ${innerEnd.y}`,
    `A ${innerRadius} ${innerRadius} 0 ${largeArcFlag} 0 ${innerStart.x} ${innerStart.y}`,
    "Z",
  ].join(" ");
}

function polarToCartesian(centerX: number, centerY: number, radius: number, angle: number) {
  const radians = ((angle - 90) * Math.PI) / 180;
  return {
    x: centerX + radius * Math.cos(radians),
    y: centerY + radius * Math.sin(radians),
  };
}

const styles = StyleSheet.create({
  centerIcon: {
    alignItems: "center",
    backgroundColor: "#FFFFFF",
    borderRadius: 999,
    height: 64,
    justifyContent: "center",
    shadowColor: "#1F2933",
    shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.06,
    shadowRadius: 8,
    width: 64,
  },
  shell: {
    alignItems: "center",
    justifyContent: "center",
  },
});
