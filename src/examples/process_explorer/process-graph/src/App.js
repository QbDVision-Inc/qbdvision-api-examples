import './App.css';
import React, {useEffect} from 'react';
import {Network} from "vis-network/peer";
import "vis-network/styles/vis-network.css";
import ProcessExplorerDataLoader from "./apiLib/process_explorer_data_loader";

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
        enabled: false,
        stabilization: {
            enabled: true,
            onlyDynamicEdges: false,
            fit: true,
        }
    }
};

const processExplorerDataLoader = new ProcessExplorerDataLoader();

function App() {

    /**
     * This function fills the DataSets. These DataSets will update the network.
     */
    async function loadNetwork() {
        const data = await processExplorerDataLoader.getData();
        console.log("Data loaded:", data);

        const container = document.getElementById("processExplorerMapDiv");
        new Network(container, data, GRAPH_OPTIONS);
    }

    useEffect(() => {
        loadNetwork();
    }, []);

    return (
        <div className="App">
            <div id="processExplorerMapContainer">
                <div id="processExplorerMapDiv"/>
            </div>
        </div>
    );
}

export default App;
