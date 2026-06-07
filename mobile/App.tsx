import { StatusBar, StyleSheet, View } from "react-native";

import { HomeScreen } from "./src/screens/HomeScreen";

export default function App() {
  return (
    <View style={styles.root}>
      <StatusBar barStyle="dark-content" />
      <HomeScreen />
    </View>
  );
}

const styles = StyleSheet.create({
  root: {
    flex: 1,
    backgroundColor: "#F7F7F4",
  },
});
