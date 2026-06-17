import { useMemo, useState, type ReactNode } from "react";
import type { LayoutChangeEvent } from "react-native";
import { Pressable, StyleSheet, Text, View } from "react-native";
import Svg, { Path } from "react-native-svg";

import { AppScreen } from "../components/ui/AppScreen";
import { EmptyState } from "../components/ui/EmptyState";
import { SectionHeader } from "../components/ui/SectionHeader";
import { colors } from "../theme/colors";

export type MealPlanTab = "mealPlan" | "groceryList";

const TAB_CONNECTOR_OVERHANG = 42;
const TAB_CONNECTOR_END_EXTENSION = 20;
const TAB_CONNECTOR_RADIUS = 14;
const TAB_CONNECTOR_SHIFT_X = 0;
const TAB_GAP = 15;

type MealPlanPageProps = {
  daySelector?: ReactNode;
  feedbackContent?: ReactNode;
  generationControls: ReactNode;
  groceryContent?: ReactNode;
  hasMembers: boolean;
  hasPlan: boolean;
  isSetupComplete: boolean;
  mealPlanContent?: ReactNode;
  messagesContent?: ReactNode;
  onGoToHousehold: () => void;
  onSelectTab: (tab: MealPlanTab) => void;
  profileSelector?: ReactNode;
  scrollToTopSignal?: number;
  selectedTab: MealPlanTab;
};

export function MealPlanPage({
  daySelector,
  feedbackContent,
  generationControls,
  groceryContent,
  hasMembers,
  hasPlan,
  isSetupComplete,
  mealPlanContent,
  messagesContent,
  onGoToHousehold,
  onSelectTab,
  profileSelector,
  scrollToTopSignal,
  selectedTab,
}: MealPlanPageProps) {
  const [tabsWidth, setTabsWidth] = useState(0);

  const handleTabsLayout = (event: LayoutChangeEvent) => {
    const nextWidth = event.nativeEvent.layout.width;
    setTabsWidth((currentWidth) => (Math.abs(currentWidth - nextWidth) > 0.5 ? nextWidth : currentWidth));
  };

  if (!isSetupComplete) {
    return (
      <AppScreen scrollToTopSignal={scrollToTopSignal}>
        <EmptyState
          actionLabel="Go to Account Setup"
          onAction={onGoToHousehold}
          text="Create an account to start planning meals for your household."
          title="Set up your account first"
        />
      </AppScreen>
    );
  }

  return (
    <AppScreen
      contentContainerStyle={styles.screenContainer}
      scrollToTopSignal={scrollToTopSignal}
    >
      {!hasMembers ? (
        <EmptyState
          actionLabel="Add Member Profile"
          onAction={onGoToHousehold}
          text="Meal plans are generated from member profiles, nutrition goals and preferences. Add at least one household member before generating a menu."
          title="No household members yet"
        />
      ) : (
        <>
          <View style={styles.generateCard}>
            <SectionHeader title="Generate meal plan" />
            <Text style={styles.bodyText}>Plan balanced meals for your household.</Text>
            {generationControls}
          </View>

          {messagesContent}

          {hasPlan ? (
            <>
              <View onLayout={handleTabsLayout} style={styles.tabsFrame}>
                <TabConnector selectedTab={selectedTab} width={tabsWidth} />
                <View style={styles.tabs}>
                  <TabButton
                    active={selectedTab === "mealPlan"}
                    label="Meal Plan"
                    onPress={() => onSelectTab("mealPlan")}
                  />
                  <TabButton
                    active={selectedTab === "groceryList"}
                    label="Grocery List"
                    onPress={() => onSelectTab("groceryList")}
                  />
                </View>
              </View>

              {selectedTab === "mealPlan" ? (
                <View style={styles.section}>
                  {profileSelector ? (
                    <View style={styles.viewerBlock}>
                      {profileSelector}
                    </View>
                  ) : null}
                  {daySelector}
                  {mealPlanContent}
                  {feedbackContent}
                </View>
              ) : (
                <View style={styles.section}>{groceryContent}</View>
              )}
            </>
          ) : (
            <EmptyState
              text="Generate a meal plan to review meals, cooking details, feedback, alternatives and grocery output."
              title="No meal plan yet"
            />
          )}
        </>
      )}
    </AppScreen>
  );
}

function TabConnector({ selectedTab, width }: { selectedTab: MealPlanTab; width: number }) {
  const path = useMemo(() => {
    if (width <= 0) {
      return "";
    }

    const tabWidth = (width - TAB_GAP) / 2;
    const svgWidth = width + TAB_CONNECTOR_OVERHANG * 2 + TAB_CONNECTOR_END_EXTENSION * 2;
    const leftTabRight = TAB_CONNECTOR_END_EXTENSION + TAB_CONNECTOR_OVERHANG + tabWidth;
    const gapCenter = leftTabRight + TAB_GAP / 2;
    const startX = 0;
    const endX = svgWidth;
    const topY = 7;
    const bottomY = 63;

    if (selectedTab === "mealPlan") {
      const turnX = gapCenter;
      return [
        `M ${startX} ${topY}`,
        `H ${turnX - TAB_CONNECTOR_RADIUS}`,
        `Q ${turnX} ${topY} ${turnX} ${topY + TAB_CONNECTOR_RADIUS}`,
        `V ${bottomY - TAB_CONNECTOR_RADIUS}`,
        `Q ${turnX} ${bottomY} ${turnX + TAB_CONNECTOR_RADIUS} ${bottomY}`,
        `H ${endX}`,
      ].join(" ");
    }

    const turnX = gapCenter;
    return [
      `M ${startX} ${bottomY}`,
      `H ${turnX - TAB_CONNECTOR_RADIUS}`,
      `Q ${turnX} ${bottomY} ${turnX} ${bottomY - TAB_CONNECTOR_RADIUS}`,
      `V ${topY + TAB_CONNECTOR_RADIUS}`,
      `Q ${turnX} ${topY} ${turnX + TAB_CONNECTOR_RADIUS} ${topY}`,
      `H ${endX}`,
    ].join(" ");
  }, [selectedTab, width]);

  if (!path) {
    return null;
  }

  const svgWidth = width + TAB_CONNECTOR_OVERHANG * 2 + TAB_CONNECTOR_END_EXTENSION * 2;
  const connectorLeft = -TAB_CONNECTOR_OVERHANG + TAB_CONNECTOR_SHIFT_X - TAB_CONNECTOR_END_EXTENSION;

  return (
    <Svg
      height={72}
      pointerEvents="none"
      style={[styles.tabConnector, { left: connectorLeft }]}
      viewBox={`0 0 ${svgWidth} 72`}
      width={svgWidth}
    >
      <Path
        d={path}
        fill="none"
        stroke="#BFD8A8"
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth={3}
      />
    </Svg>
  );
}

function TabButton({
  active,
  label,
  onPress,
}: {
  active: boolean;
  label: string;
  onPress: () => void;
}) {
  return (
    <Pressable
      accessibilityRole="button"
      onPress={onPress}
      style={({ pressed }) => [
        styles.tabButton,
        active ? styles.tabButtonActive : null,
        pressed ? styles.pressed : null,
      ]}
    >
      <Text style={[styles.tabText, active ? styles.tabTextActive : null]}>{label}</Text>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  bodyText: {
    color: colors.muted,
    fontSize: 14,
    lineHeight: 19,
  },
  generateCard: {
    backgroundColor: colors.card,
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    gap: 9,
    padding: 14,
  },
  pressed: {
    opacity: 0.82,
  },
  screenContainer: {
    gap: 14,
    paddingTop: 14,
  },
  section: {
    gap: 10,
  },
  tabButton: {
    alignItems: "center",
    borderColor: colors.accent,
    borderRadius: 8,
    borderWidth: 1,
    flex: 1,
    justifyContent: "center",
    minHeight: 42,
  },
  tabButtonActive: {
    backgroundColor: colors.accent,
  },
  tabText: {
    color: colors.accent,
    fontSize: 14,
    fontWeight: "900",
  },
  tabTextActive: {
    color: "#FFFFFF",
  },
  tabConnector: {
    position: "absolute",
    top: -14,
  },
  tabsFrame: {
    paddingBottom: 6,
    position: "relative",
  },
  tabs: {
    flexDirection: "row",
    gap: TAB_GAP,
    position: "relative",
    zIndex: 1,
  },
  viewerBlock: {
    gap: 0,
  },
});
