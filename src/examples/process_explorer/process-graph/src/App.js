import './App.css';
import React from 'react';
import { Network } from "vis-network/peer";
import "vis-network/styles/vis-network.css";
import ProcessExplorerAPIProxy from "./apiLib/process_explorer_api_proxy";

const API_KEY = "b43e671d58d64d389407530f1ff3f06b";
const BASE_URL = "http://localhost:3000/"
const GRAPH_OPTIONS = {
  nodes: {
    shape: "dot",
    scaling: {
      min: 25,
      max: 45
    },
    font: {
      multi: false,
      size: 16,
      face: "Open Sans",
    },
    borderWidth: 2,
    color: {
      background: "#3366ff",
      border: "#002db3",
      highlight: {
        background: "#002699",
        border: "#002db3"
      },
      hover: {
        background: "#7093ff",
        border: "#006551"
      }
    }
  },
  edges: {
    width: 2,
    arrows: {
      to: {
        enabled: true
      }
    },
    color: {
      inherit: false
    }
  },
  layout: {
    randomSeed: 12
  },
  interaction: {
    hover: true,
    multiselect: true,
    tooltipDelay: 300
  },
  physics: {
    enabled: true,
    stabilization: {
      enabled: true,
      onlyDynamicEdges: false,
      fit: true,
    }
  }
};
const PROJECT_ID = 1;
const PROCESS_ID = 1;
const openAPIProxy = new ProcessExplorerAPIProxy(API_KEY, BASE_URL);

function App() {

  /**
   * This function fills the DataSets. These DataSets will update the network.
   */
  async function sync() {
    let data = await openAPIProxy.getProcessExplorerData(PROJECT_ID, PROCESS_ID);

    const container = document.getElementById("processExplorerMapDiv");
    new Network(container, data, GRAPH_OPTIONS);
  }

  return (
    <div className="App">
      <div>
        <button title="Sync"
                onClick={sync}>
          Sync
        </button>
      </div>
      <div id="processExplorerMapContainer">
        <div id="processExplorerMapDiv"/>
      </div>
    </div>
  );
}

export default App;
