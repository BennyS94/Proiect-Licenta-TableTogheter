import type { ReactNode } from "react";
import { Pressable, StyleSheet, Text, View } from "react-native";

import { AppCard } from "../components/ui/AppCard";
import { AppScreen } from "../components/ui/AppScreen";
import { EmptyState } from "../components/ui/EmptyState";
import { SectionHeader } from "../components/ui/SectionHeader";
import { colors } from "../theme/colors";

export type MealPlanTab = "mealPlan" | "groceryList";

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
  selectedTab,
}: MealPlanPageProps) {
  if (!isSetupComplete) {
    return (
      <AppScreen>
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
    <AppScreen>
      <View style={styles.header}>
        <Text style={styles.title}>Meal Plan</Text>
      </View>

      {!hasMembers ? (
        <EmptyState
          actionLabel="Add Member Profile"
          onAction={onGoToHousehold}
          text="Meal plans are generated from member profiles, nutrition goals and preferences. Add at least one household member before generating a menu."
          title="No household members yet"
        />
      ) : (
        <>
          <AppCard>
            <SectionHeader title="Generate meal plan" />
            <Text style={styles.bodyText}>Plan balanced meals for your household.</Text>
            {generationControls}
          </AppCard>

          {messagesContent}

          {hasPlan ? (
            <>
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
    fontSize: 15,
    lineHeight: 21,
  },
  header: {
    alignItems: "center",
    gap: 4,
    paddingTop: 4,
  },
  pressed: {
    opacity: 0.82,
  },
  section: {
    gap: 14,
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
  tabs: {
    flexDirection: "row",
    gap: 10,
  },
  title: {
    color: colors.text,
    fontSize: 30,
    fontWeight: "900",
    textAlign: "center",
  },
  viewerBlock: {
    gap: 0,
  },
});
