import { StatusBar, StyleSheet, View } from "react-native";

import { HomeScreen } from "./src/screens/HomeScreen";
import { colors } from "./src/theme/colors";

export default function App() {
  return (
    <View style={styles.root}>
      <StatusBar
        backgroundColor={colors.background}
        barStyle="dark-content"
        translucent={false}
      />
      <HomeScreen />
    </View>
  );
}

const styles = StyleSheet.create({
  root: {
    backgroundColor: colors.background,
    flex: 1,
  },
});
