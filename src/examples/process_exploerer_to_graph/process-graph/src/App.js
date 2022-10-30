import './App.css';
import React from 'react';
import {Network, parseGephiNetwork} from "vis-network/peer";
import { DataSet } from "vis-data/peer";
import ProcessExplorerAPIProxy from "./apiLib/process_explorer_api_proxy";

const API_KEY = "fa39b8710a184a9a9ba8a06877866c10";
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
const PROJECT_ID = 2;
const PROCESS_ID = 2;
const openAPIProxy = new ProcessExplorerAPIProxy(API_KEY, BASE_URL);

function App() {

  /**
   * This function fills the DataSets. These DataSets will update the network.
   */
  async function sync() {
    nodes.clear();
    edges.clear();

    let treeData = await openAPIProxy.getProcessExplorerData(PROJECT_ID, PROCESS_ID);
    //console.log(treeData);

    let parsed = parseGephiNetwork(treeData, {
      fixed: false,
      parseColor: false,
    });

    // add the parsed data to the DataSets.
    nodes.add(parsed.nodes);
    edges.add(parsed.edges);

    let network = new Network(processExplorerMapDiv, {nodes, edges}, GRAPH_OPTIONS);
    network.fit(); // zoom to fit
  }

  let processExplorerMapDiv = React.createRef();
  let nodes = new DataSet();
  let edges = new DataSet();

  return (
    <div className="App">
      <div>
        <button title="Sync"
                onClick={sync}/>
      </div>
      <div id="processExplorerMapContainer">
        <div id="processExplorerMapDiv"
             ref={processExplorerMapDiv}
        />
      </div>
    </div>
  );
}

export default App;
